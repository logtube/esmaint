package tasks

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/guoyk93/conc"
	"github.com/olivere/elastic"
	"log"
	"net/http"
	"net/url"
	"strings"
	"time"
)

const (
	StatusOpen           = "open"
	CodecBestCompression = "best_compression"
)

type ElasticsearchIndex struct {
	Index  string // 索引名
	Open   bool   // 是否已经打开
	Merged bool   // 是否已经完全合并
}

func ListIndices(client *elastic.Client, out *[]ElasticsearchIndex) conc.Task {
	return conc.TaskFunc(func(ctx context.Context) (err error) {
		var res elastic.CatIndicesResponse
		if res, err = client.CatIndices().Columns(
			"index",
			"status",
			"health",
			"pri",
			"pri.segments.count",
		).Do(ctx); err != nil {
			return
		}
		*out = []ElasticsearchIndex{}
		for _, ci := range res {
			*out = append(*out, ElasticsearchIndex{
				Index:  ci.Index,
				Open:   ci.Status == StatusOpen,
				Merged: ci.Pri >= ci.PriSegmentsCount,
			})
		}
		return
	})
}

func CloseIndex(client *elastic.Client, index string) conc.Task {
	return conc.TaskFunc(func(ctx context.Context) (err error) {
		_, err = client.CloseIndex(index).Do(ctx)
		return
	})
}

func OpenIndex(client *elastic.Client, index string) conc.Task {
	return conc.TaskFunc(func(ctx context.Context) (err error) {
		_, err = client.OpenIndex(index).WaitForActiveShards("all").Do(ctx)
		return
	})
}

func GetIndexSettings(client *elastic.Client, index string, out *map[string]interface{}) conc.Task {
	return conc.TaskFunc(func(ctx context.Context) (err error) {
		var res map[string]*elastic.IndicesGetSettingsResponse
		if res, err = client.IndexGetSettings(index).FlatSettings(true).Do(ctx); err != nil {
			return
		}
		settings := res[index]
		if settings == nil {
			err = fmt.Errorf("无法找到索引 %s 的配置", index)
			return
		}
		*out = settings.Settings
		return
	})
}

func SetIndexSettings(client *elastic.Client, index string, settings map[string]interface{}) conc.Task {
	return conc.TaskFunc(func(ctx context.Context) (err error) {
		_, err = client.IndexPutSettings(index).FlatSettings(true).BodyJson(settings).Do(ctx)
		return
	})
}

func CheckIndexBestCompression(client *elastic.Client, index string, out *bool) conc.Task {
	return conc.TaskFunc(func(ctx context.Context) (err error) {
		var settings map[string]interface{}
		if err = GetIndexSettings(client, index, &settings).Do(ctx); err != nil {
			return
		}
		codec, _ := settings["index.codec"].(string)
		*out = codec == CodecBestCompression
		return
	})
}

func SetIndexBestCompression(client *elastic.Client, index string) conc.Task {
	return conc.SerialFailSafe(
		conc.Serial(
			CloseIndex(client, index),
			SetIndexSettings(client, index, map[string]interface{}{
				"index.codec": CodecBestCompression,
			}),
		),
		OpenIndex(client, index),
	)
}

func CheckIndexRoutingHDD(client *elastic.Client, index string, out *bool) conc.Task {
	return conc.TaskFunc(func(ctx context.Context) (err error) {
		var settings map[string]interface{}
		if err = GetIndexSettings(client, index, &settings).Do(ctx); err != nil {
			return
		}
		exclude, _ := settings["index.routing.allocation.exclude.disktype"].(string)
		require, _ := settings["index.routing.allocation.require.disktype"].(string)
		*out = len(exclude) == 0 && require == "hdd"
		return
	})
}

func SetIndexRoutingHDD(client *elastic.Client, index string) conc.Task {
	return SetIndexSettings(client, index, map[string]interface{}{
		"index.routing.allocation.exclude.disktype": nil,
		"index.routing.allocation.require.disktype": "hdd",
	})
}

func SetIndexReadOnly(client *elastic.Client, index string, readOnly bool) conc.Task {
	if readOnly {
		return SetIndexSettings(client, index, map[string]interface{}{
			"index.blocks.write": true,
		})
	} else {
		return SetIndexSettings(client, index, map[string]interface{}{
			"index.blocks.write": nil,
		})
	}
}

func MergeIndex(client *elastic.Client, index string) conc.Task {
	return conc.SerialFailSafe(
		conc.Serial(
			SetIndexReadOnly(client, index, true),
			conc.TaskFunc(func(ctx context.Context) (err error) {
				_, err = client.Forcemerge(index).MaxNumSegments(1).Do(ctx)
				return
			}),
		),
		SetIndexReadOnly(client, index, false),
	)
}

func DeleteIndex(client *elastic.Client, index string) conc.Task {
	return conc.TaskFunc(func(ctx context.Context) (err error) {
		_, err = client.DeleteIndex(index).Do(ctx)
		return
	})
}

type ElasticsearchRecovery struct {
	Index        string `json:"index"`
	Shard        string `json:"shard"`
	BytesPercent string `json:"bytes_percent"`
}

func (e ElasticsearchRecovery) String() string {
	return fmt.Sprintf("%s#%s (%s)", e.Index, e.Shard, e.BytesPercent)
}

func ListClusterRecoveries(client *elastic.Client, out *[]ElasticsearchRecovery) conc.Task {
	return conc.TaskFunc(func(ctx context.Context) (err error) {
		v := url.Values{}
		v.Set("format", "json")
		v.Set("active_only", "true")

		var res *elastic.Response
		if res, err = client.PerformRequest(ctx, elastic.PerformRequestOptions{
			Method: http.MethodGet,
			Path:   "/_cat/recovery",
			Params: v,
		}); err != nil {
			return
		}

		var ret []ElasticsearchRecovery
		if err = json.Unmarshal(res.Body, &ret); err != nil {
			return
		}

		*out = ret
		return
	})
}

func WaitClusterRecoveries(client *elastic.Client) conc.Task {
	return conc.TaskFunc(func(ctx context.Context) (err error) {
		tk := time.NewTicker(time.Second * 5)
		defer tk.Stop()
		var rs []ElasticsearchRecovery
		for {
			select {
			case <-tk.C:
				if err = ListClusterRecoveries(client, &rs).Do(ctx); err != nil {
					return
				}
				if len(rs) > 0 {
					sb := &strings.Builder{}
					for _, r := range rs {
						if sb.Len() > 0 {
							sb.WriteString("; ")
						}
						sb.WriteString(r.String())
					}
					log.Printf("正在等待集群恢复: %s", sb.String())
				} else {
					return
				}
			case <-ctx.Done():
				err = ctx.Err()
				return
			}
		}
	})
}
