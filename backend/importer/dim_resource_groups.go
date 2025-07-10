package importer

var (
	resourceGroupSequence = 0
	resourceGroups        map[string]int
	resourceGroupValues   []any
)

func getResourceGroupSk(row []string) int {
	if resourceGroups == nil {
		resourceGroups = make(map[string]int)
	}

	name := row[resourceGroupIndex]

	if name == "" {
		return 0
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

		return resourceGroupSequence
	}

	return existentSequence
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
