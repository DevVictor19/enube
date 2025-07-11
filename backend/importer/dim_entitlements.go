package importer

var (
	entitlementSequence = 0
	entitlements        map[string]int
	entitlementValues   []any
)

func getEntitlementSk(row []string) *int {
	entitlementId := row[entitlementIdIndex]
	description := row[entitlementDescriptionIndex]

	if entitlementId == "" {
		return nil
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

		return &entitlementSequence
	}

	return &existentSequence
}

func getEntitlementStm() string {
	table := "dim_entitlements"
	cols := []string{
		"entitlement_sk",
		"entitlement_id",
		"description",
	}
	totalVals := len(entitlementValues)
	return buildBatchInsert(table, cols, totalVals)
}

func resetEntitlementValues() {
	entitlementValues = nil
}
