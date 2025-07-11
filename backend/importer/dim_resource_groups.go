package importer

import "database/sql"

var (
	resourceGroupSequence = 0
	resourceGroups        map[string]int
	resourceGroupValues   []any
)

func getResourceGroupSk(row []string) sql.NullInt32 {
	if len(row) <= resourceGroupIndex {
		return sql.NullInt32{Valid: false}
	}

	name := row[resourceGroupIndex]

	if name == "" {
		return sql.NullInt32{Valid: false}
	}

	if resourceGroups == nil {
		resourceGroups = make(map[string]int)
	}

	existentSequence, ok := resourceGroups[name]
	if !ok {
		resourceGroupSequence++
		resourceGroups[name] = resourceGroupSequence

		resourceGroupValues = append(
			resourceGroupValues,
			resourceGroupSequence,
			name,
		)

		return sql.NullInt32{
			Valid: true,
			Int32: int32(resourceGroupSequence),
		}
	}

	return sql.NullInt32{
		Valid: true,
		Int32: int32(existentSequence),
	}
}

func getResourceGroupStm() string {
	table := "dim_resource_groups"
	cols := []string{
		"resource_group_sk",
		"name",
	}
	totalVals := len(resourceGroupValues)
	return buildBatchInsert(table, cols, totalVals)
}

func resetResourceGroupValues() {
	resourceGroupValues = nil
}
