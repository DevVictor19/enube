package importer

import (
	"database/sql"

	"github.com/DevVictor19/enube/backend/importer/helpers"
)

var (
	usageDateSequence = 0
	usageDates        map[string]int
	usageDateValues   []any
)

func getUsageDateSk(row []string) sql.NullInt32 {
	usageDate := row[usageDateIndex]

	if usageDate == "" {
		return sql.NullInt32{Valid: false}
	}

	if usageDates == nil {
		usageDates = make(map[string]int)
	}

	existentSequence, ok := usageDates[usageDate]
	if !ok {
		usageDateSequence++
		usageDates[usageDate] = usageDateSequence

		usageDateValues = append(
			usageDateValues,
			usageDateSequence,
			helpers.ToNullableDate(usageDate),
		)

		return sql.NullInt32{
			Valid: true,
			Int32: int32(usageDateSequence),
		}
	}

	return sql.NullInt32{
		Valid: true,
		Int32: int32(existentSequence),
	}
}

func getUsageDateStm() string {
	table := "dim_usage_dates"
	cols := []string{
		"usage_date_sk",
		"usage_date",
	}
	totalVals := len(usageDateValues)
	return helpers.BuildBatchInsert(table, cols, totalVals)
}

func resetUsageDateValues() {
	usageDateValues = nil
}
