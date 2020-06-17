package esmaint

import (
	"errors"
	"gopkg.in/yaml.v2"
	"io/ioutil"
	"sort"
	"strconv"
	"strings"
)

var (
	ErrIndexIgnored = errors.New("忽略该索引")
)

type Rule struct {
	Warm   int64
	Move   int64
	Cold   int64
	Delete int64
}

func decodeRule(splits []string, out ...*int64) (err error) {
	for i, s := range splits {
		s = strings.TrimSpace(s)
		if s == "-" {
			*out[i] = 0
			continue
		}
		if *out[i], err = strconv.ParseInt(s, 10, 64); err != nil {
			return
		}
	}
	return
}

func ParseRule(s string) (rule Rule, err error) {
	splits := strings.Split(s, ",")
	if len(splits) != 4 {
		err = errors.New("rule 格式错误")
		return
	}
	if err = decodeRule(splits, &rule.Warm, &rule.Move, &rule.Cold, &rule.Delete); err != nil {
		return
	}
	return
}

type Rules map[string]Rule

type Conf struct {
	Elasticsearch struct {
		URL string `yaml:"url"` // ES 集群的 URL
	} `yaml:"elasticsearch"`

	COS struct {
		URL       string `yaml:"url"`        // COS 存储桶的 URL
		SecretID  string `yaml:"secret_id"`  // COS 访问秘钥ID
		SecretKey string `yaml:"secret_key"` // COS 访问秘钥
		Check     string `yaml:"check"`      // COS 检查规则，"1:5" 代表检查 1 天前 到 5 天前的索引是否已经保存在 COS 上
	} `yaml:"cos"`

	Dir struct {
		Workspace string `yaml:"workspace"` // 工作空间目录，工作空间用于将 索引迁移至 COS
		Templates string `yaml:"templates"` // 模板目录，包含一堆 .json 文件
	} `yaml:"dir"`

	Indices struct {
		Ignores []string          `yaml:"ignores"` // 忽略索引 (包含)
		Rules   map[string]string `yaml:"rules"`   // 索引规则 (前缀)
	} `yaml:"indices"`
}

func LoadConf(filename string) (conf Conf, err error) {
	var buf []byte
	if buf, err = ioutil.ReadFile(filename); err != nil {
		return
	}
	if err = yaml.Unmarshal(buf, &conf); err != nil {
		return
	}
	return
}

func (c Conf) FindRule(index string) (rule Rule, err error) {
	if strings.HasPrefix(index, ".") {
		err = ErrIndexIgnored
		return
	}
	for _, s := range c.Indices.Ignores {
		if strings.Contains(index, s) {
			err = ErrIndexIgnored
			return
		}
	}

	// 排序，让更长的规则放在前面更早匹配
	var names []string
	for name := range c.Indices.Rules {
		names = append(names, name)
	}
	sort.Sort(sort.Reverse(sort.StringSlice(names)))

	for _, name := range names {
		if strings.HasPrefix(index, name) {
			if rule, err = ParseRule(c.Indices.Rules[name]); err != nil {
				return
			}
			return
		}
	}

	return
}
