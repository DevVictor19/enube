package importer

var (
	chargeTypeSequence = 0
	chargeTypes        map[string]int
	chargeTypeValues   []any
)

func getChargeTypeSk(row []string) *int {
	chargeType := row[chargeTypeIndex]

	if chargeType == "" {
		return nil
	}

	if chargeTypes == nil {
		chargeTypes = make(map[string]int)
	}

	existentSequence, ok := chargeTypes[chargeType]
	if !ok {
		chargeTypeSequence++
		chargeTypes[chargeType] = chargeTypeSequence

		chargeTypeValues = append(
			chargeTypeValues,
			chargeTypeSequence,
			chargeType,
		)

		return &chargeTypeSequence
	}

	return &existentSequence
}

func getChargeTypeStm() string {
	table := "dim_charge_types"
	cols := []string{
		"charge_type_sk",
		"type",
	}
	totalVals := len(chargeTypeValues)
	return buildBatchInsert(table, cols, totalVals)
}

func resetChargeTypeValues() {
	chargeTypeValues = nil
}
