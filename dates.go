package esmaint

import "time"

const (
	IndexDateSuffixLayout = "2006-01-02"
)

func DateFromIndex(index string) (time.Time, bool) {
	if len(index) >= len(IndexDateSuffixLayout) {
		if date, err := time.Parse(
			IndexDateSuffixLayout,
			index[len(index)-len(IndexDateSuffixLayout):],
		); err == nil {
			return date, true
		}
	}

	return time.Time{}, false
}
