package esmaint

import (
	"github.com/stretchr/testify/assert"
	"path/filepath"
	"testing"
)

func TestParseRule(t *testing.T) {
	var err error
	var s string
	var r Rule
	s = "5, 8, 9, 10"
	r, err = ParseRule(s)
	assert.NoError(t, err)
	assert.Equal(t, int64(5), r.Warm)
	assert.Equal(t, int64(8), r.Move)
	assert.Equal(t, int64(9), r.Cold)
	assert.Equal(t, int64(10), r.Delete)

	s = "5, 8, -, 10"
	r, err = ParseRule(s)
	assert.NoError(t, err)
	assert.Equal(t, int64(5), r.Warm)
	assert.Equal(t, int64(8), r.Move)
	assert.Equal(t, int64(0), r.Cold)
	assert.Equal(t, int64(10), r.Delete)
}

func TestLoadConf(t *testing.T) {
	var err error
	var conf Conf
	var rule Rule
	conf, err = LoadConf(filepath.Join("testdata", "conf.yml"))
	assert.NoError(t, err)
	assert.Equal(t, "http://127.0.0.1:9200", conf.Elasticsearch.URL)
	assert.Equal(t, "/workspace", conf.Dir.Workspace)
	assert.Equal(t, "/etc/esmaint.tmpl.d", conf.Dir.Templates)
	assert.Equal(t, 1, len(conf.Indices.Ignores))
	rule, err = conf.FindRule("info-prod-2020-05-05")
	assert.NoError(t, err)
	assert.Equal(t, int64(0), rule.Delete)
	rule, err = conf.FindRule("info-prod-2019-12-12")
	assert.Equal(t, ErrIndexIgnored, err)
	rule, err = conf.FindRule(".kibana")
	assert.Equal(t, ErrIndexIgnored, err)
}
