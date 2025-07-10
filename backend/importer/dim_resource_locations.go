package importer

var (
	resourceLocationSequence = 0
	resourceLocations        map[string]int
	resourceLocationValues   []any
)

func getResourceLocationSk(row []string) int {
	if resourceLocations == nil {
		resourceLocations = make(map[string]int)
	}

	location := row[resourceLocationIndex]

	existentSequence, ok := resourceLocations[location]
	if !ok {
		resourceLocationSequence++
		resourceLocations[location] = resourceLocationSequence

		resourceLocationValues = append(
			resourceLocationValues,
			resourceLocationSequence,
			location,
		)

		return resourceLocationSequence
	}

	return existentSequence
}

func getResourceLocationStm() string {
	table := "dim_resource_locations"
	cols := []string{
		"resource_location_sk",
		"location",
	}
	totalVals := len(resourceLocationValues)
	return buildBatchInsert(table, cols, totalVals)
}
