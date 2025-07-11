package importer

import (
	"database/sql"

	"github.com/DevVictor19/enube/backend/importer/helpers"
)

var (
	entitlementSequence = 0
	entitlements        map[string]int
	entitlementValues   []any
)

func getEntitlementSk(row []string) sql.NullInt32 {
	if len(row) <= entitlementDescriptionIndex {
		return sql.NullInt32{Valid: false}
	}

	entitlementId := row[entitlementIdIndex]
	description := row[entitlementDescriptionIndex]

	if entitlementId == "" {
		return sql.NullInt32{Valid: false}
	}

	if entitlements == nil {
		entitlements = make(map[string]int)
	}

	existentSequence, ok := entitlements[entitlementId]
	if !ok {
		entitlementSequence++
		entitlements[entitlementId] = entitlementSequence

		entitlementValues = append(
			entitlementValues,
			entitlementSequence,
			entitlementId,
			description,
		)

		return sql.NullInt32{
			Valid: true,
			Int32: int32(entitlementSequence),
		}
	}

	return sql.NullInt32{
		Valid: true,
		Int32: int32(existentSequence),
	}
}

func getEntitlementStm() string {
	table := "dim_entitlements"
	cols := []string{
		"entitlement_sk",
		"entitlement_id",
		"description",
	}
	totalVals := len(entitlementValues)
	return helpers.BuildBatchInsert(table, cols, totalVals)
}

func resetEntitlementValues() {
	entitlementValues = nil
}
