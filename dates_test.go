package esmaint

import (
	"github.com/stretchr/testify/assert"
	"testing"
	"time"
)

func TestDateFromIndex(t *testing.T) {
	var date time.Time
	var ok bool
	date, ok = DateFromIndex("hello-2020-02-02")
	assert.True(t, ok)
	assert.Equal(t, time.Date(2020, 2, 2, 0, 0, 0, 0, time.UTC), date)
	date, ok = DateFromIndex("hello-2020-02-2")
	assert.False(t, ok)
}
