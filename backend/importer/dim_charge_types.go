package importer

import "database/sql"

var (
	chargeTypeSequence = 0
	chargeTypes        map[string]int
	chargeTypeValues   []any
)

func getChargeTypeSk(row []string) sql.NullInt32 {
	if len(row) <= chargeTypeIndex {
		return sql.NullInt32{Valid: false}
	}

	chargeType := row[chargeTypeIndex]
	if chargeType == "" {
		return sql.NullInt32{Valid: false}
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

		return sql.NullInt32{
			Valid: true,
			Int32: int32(chargeTypeSequence),
		}
	}

	return sql.NullInt32{
		Valid: true,
		Int32: int32(existentSequence),
	}
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
