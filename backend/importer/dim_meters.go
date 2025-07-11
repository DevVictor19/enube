package importer

import (
	"database/sql"

	"github.com/DevVictor19/enube/backend/importer/helpers"
)

var (
	meterSequence = 0
	meters        map[string]int
	meterValues   []any
)

func getMeterSk(row []string) sql.NullInt32 {
	if len(row) <= meterUnitIndex {
		return sql.NullInt32{Valid: false}
	}

	meterId := row[meterIdIndex]
	name := row[meterNameIndex]
	category := row[meterCategoryIndex]
	meterType := row[meterTypeIndex]
	subcategory := row[meterSubCategoryIndex]
	region := row[meterRegionIndex]
	unit := row[meterUnitIndex]

	if meterId == "" {
		return sql.NullInt32{Valid: false}
	}

	if meters == nil {
		meters = make(map[string]int)
	}

	existentSequence, ok := meters[meterId]
	if !ok {
		meterSequence++
		meters[meterId] = meterSequence

		meterValues = append(
			meterValues,
			meterSequence,
			meterId,
			name,
			category,
			meterType,
			subcategory,
			region,
			unit,
		)

		return sql.NullInt32{
			Valid: true,
			Int32: int32(meterSequence),
		}
	}

	return sql.NullInt32{
		Valid: true,
		Int32: int32(existentSequence),
	}
}

func getMeterStm() string {
	table := "dim_meters"
	cols := []string{
		"meter_sk",
		"meter_id",
		"name",
		"category",
		"type",
		"subcategory",
		"region",
		"unit",
	}
	totalVals := len(meterValues)
	return helpers.BuildBatchInsert(table, cols, totalVals)
}

func resetMeterValues() {
	meterValues = nil
}
