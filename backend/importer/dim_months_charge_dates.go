package importer

import "fmt"

var (
	monthsChargeDateSequence = 0
	monthsChargeDates        map[string]int
	monthsChargeDateValues   []any
)

func getMonthsChargeDateSk(row []string) *int {
	if monthsChargeDates == nil {
		monthsChargeDates = make(map[string]int)
	}

	chargeStartDate := row[chargeStartDateIndex]
	chargeEndDate := row[chargeEndDateIndex]

	if chargeStartDate == "" || chargeEndDate == "" {
		return nil
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

		return &monthsChargeDateSequence
	}

	return &existentSequence
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
