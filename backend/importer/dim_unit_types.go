package importer

import (
	"database/sql"

	"github.com/DevVictor19/enube/backend/importer/helpers"
)

var (
	unitTypeSequence = 0
	unitTypes        map[string]int
	unitTypeValues   []any
)

func getUnitTypeSk(row []string) sql.NullInt32 {
	unitType := row[unitTypeIndex]

	if unitType == "" {
		return sql.NullInt32{
			Valid: false,
		}
	}

	if unitTypes == nil {
		unitTypes = make(map[string]int)
	}

	existentSequence, ok := unitTypes[unitType]
	if !ok {
		unitTypeSequence++
		unitTypes[unitType] = unitTypeSequence

		unitTypeValues = append(
			unitTypeValues,
			unitTypeSequence,
			unitType,
		)

		return sql.NullInt32{
			Valid: true,
			Int32: int32(unitTypeSequence),
		}
	}

	return sql.NullInt32{
		Valid: true,
		Int32: int32(existentSequence),
	}
}

func getUnitTypeStm() string {
	table := "dim_unit_types"
	cols := []string{
		"unit_type_sk",
		"type",
	}
	totalVals := len(unitTypeValues)
	return helpers.BuildBatchInsert(table, cols, totalVals)
}

func resetUnitTypeValues() {
	unitTypeValues = nil
}
