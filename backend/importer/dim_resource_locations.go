package importer

import (
	"database/sql"

	"github.com/DevVictor19/enube/backend/importer/helpers"
)

var (
	resourceLocationSequence = 0
	resourceLocations        map[string]int
	resourceLocationValues   []any
)

func getResourceLocationSk(row []string) sql.NullInt32 {
	if len(row) <= resourceLocationIndex {
		return sql.NullInt32{Valid: false}
	}

	location := row[resourceLocationIndex]

	if location == "" {
		return sql.NullInt32{Valid: false}
	}

	if resourceLocations == nil {
		resourceLocations = make(map[string]int)
	}

	existentSequence, ok := resourceLocations[location]
	if !ok {
		resourceLocationSequence++
		resourceLocations[location] = resourceLocationSequence

		resourceLocationValues = append(
			resourceLocationValues,
			resourceLocationSequence,
			location,
		)

		return sql.NullInt32{
			Valid: true,
			Int32: int32(resourceLocationSequence),
		}
	}

	return sql.NullInt32{
		Valid: true,
		Int32: int32(existentSequence),
	}
}

func getResourceLocationStm() string {
	table := "dim_resource_locations"
	cols := []string{
		"resource_location_sk",
		"location",
	}
	totalVals := len(resourceLocationValues)
	return helpers.BuildBatchInsert(table, cols, totalVals)
}

func resetResourceLocationValues() {
	resourceLocationValues = nil
}
