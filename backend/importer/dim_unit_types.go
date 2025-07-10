package importer

var (
	unitTypeSequence = 0
	unitTypes        map[string]int
	unitTypeValues   []any
)

func getUnitTypeSk(row []string) int {
	if unitTypes == nil {
		unitTypes = make(map[string]int)
	}

	unitType := row[unitTypeIndex]

	existentSequence, ok := unitTypes[unitType]
	if !ok {
		unitTypeSequence++
		unitTypes[unitType] = unitTypeSequence

		unitTypeValues = append(
			unitTypeValues,
			unitTypeSequence,
			unitType,
		)

		return unitTypeSequence
	}

	return existentSequence
}

func getUnitTypeStm() string {
	table := "dim_unit_types"
	cols := []string{
		"unit_type_sk",
		"type",
	}
	totalVals := len(unitTypeValues)
	return buildBatchInsert(table, cols, totalVals)
}
