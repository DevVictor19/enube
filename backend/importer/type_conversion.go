package importer

import (
	"strconv"
	"time"
)

func toNullableInt64(s string) *int64 {
	if s == "" {
		return nil
	}
	i, err := strconv.ParseInt(s, 10, 64)
	if err != nil {
		return nil
	}
	return &i
}

func toNullableFloat64(s string) *float64 {
	if s == "" {
		return nil
	}
	f, err := strconv.ParseFloat(s, 64)
	if err != nil {
		return nil
	}
	return &f
}

func toNullableDate(s string) *time.Time {
	if s == "" {
		return nil
	}
	layout := "02-01-06"
	t, err := time.Parse(layout, s)
	if err != nil {
		return nil
	}
	return &t
}
