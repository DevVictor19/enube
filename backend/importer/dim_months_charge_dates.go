package importer

import (
	"database/sql"
	"fmt"
)

var (
	monthsChargeDateSequence = 0
	monthsChargeDates        map[string]int
	monthsChargeDateValues   []any
)

func getMonthsChargeDateSk(row []string) sql.NullInt32 {
	if len(row) <= chargeEndDateIndex {
		return sql.NullInt32{Valid: false}
	}

	chargeStartDate := row[chargeStartDateIndex]
	chargeEndDate := row[chargeEndDateIndex]

	if chargeStartDate == "" || chargeEndDate == "" {
		return sql.NullInt32{Valid: false}
	}

	if monthsChargeDates == nil {
		monthsChargeDates = make(map[string]int)
	}

	key := fmt.Sprintf("%s|%s", chargeStartDate, chargeEndDate)

	existentSequence, ok := monthsChargeDates[key]
	if !ok {
		monthsChargeDateSequence++
		monthsChargeDates[key] = monthsChargeDateSequence

		monthsChargeDateValues = append(
			monthsChargeDateValues,
			monthsChargeDateSequence,
			toNullableDate(chargeStartDate),
			toNullableDate(chargeEndDate),
		)

		return sql.NullInt32{
			Valid: true,
			Int32: int32(monthsChargeDateSequence),
		}
	}

	return sql.NullInt32{
		Valid: true,
		Int32: int32(existentSequence),
	}
}

func getMonthsChargeDateStm() string {
	table := "dim_months_charge_dates"
	cols := []string{
		"months_charge_date_sk",
		"charge_start_date",
		"charge_end_date",
	}
	totalVals := len(monthsChargeDateValues)
	return buildBatchInsert(table, cols, totalVals)
}

func resetMonthsChargeDateValues() {
	monthsChargeDateValues = nil
}
