package importer

var (
	meterSequence = 0
	meters        map[string]int
	meterValues   []any
)

func getMeterSk(row []string) *int {
	if meters == nil {
		meters = make(map[string]int)
	}

	meterId := row[meterIdIndex]
	name := row[meterNameIndex]
	category := row[meterCategoryIndex]
	meterType := row[meterTypeIndex]
	subcategory := row[meterSubCategoryIndex]
	region := row[meterRegionIndex]
	unit := row[meterUnitIndex]

	if meterId == "" {
		return nil
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

		return &meterSequence
	}

	return &existentSequence
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
	return buildBatchInsert(table, cols, totalVals)
}

func resetMeterValues() {
	meterValues = nil
}
