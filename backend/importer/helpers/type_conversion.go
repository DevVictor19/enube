package helpers

import (
	"database/sql"
	"strconv"
	"time"
)

func ToNullableInt64(s string) sql.NullInt64 {
	if s == "" {
		return sql.NullInt64{
			Valid: false,
		}
	}
	i, err := strconv.ParseInt(s, 10, 64)
	if err != nil {
		return sql.NullInt64{
			Valid: false,
		}
	}
	return sql.NullInt64{
		Valid: true,
		Int64: i,
	}
}

func ToNullableFloat64(s string) sql.NullFloat64 {
	if s == "" {
		return sql.NullFloat64{
			Valid: false,
		}
	}
	f, err := strconv.ParseFloat(s, 64)
	if err != nil {
		return sql.NullFloat64{
			Valid: false,
		}
	}
	return sql.NullFloat64{
		Valid:   true,
		Float64: f,
	}
}

func ToNullableDate(s string) sql.NullTime {
	if s == "" {
		return sql.NullTime{
			Valid: false,
		}
	}
	layout := "02-01-06"
	t, err := time.Parse(layout, s)
	if err != nil {
		return sql.NullTime{
			Valid: false,
		}
	}
	return sql.NullTime{
		Valid: true,
		Time:  t,
	}
}
