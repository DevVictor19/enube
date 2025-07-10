package importer

import (
	"strconv"
	"strings"
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

	parts := strings.Split(s, "/")
	if len(parts) != 3 {
		return nil
	}

	mes, err1 := strconv.Atoi(parts[0])
	dia, err2 := strconv.Atoi(parts[1])
	ano, err3 := strconv.Atoi(parts[2])

	if err1 != nil || err2 != nil || err3 != nil {
		return nil
	}

	t := time.Date(ano, time.Month(mes), dia, 0, 0, 0, 0, time.UTC)
	return &t
}
