package importer

import "database/sql"

var (
	availabilitySequence = 0
	availabilities       map[string]int
	availabilityValues   []any
)

func getAvailabilitySk(row []string) sql.NullInt32 {
	if len(row) <= availabilityIdIndex {
		return sql.NullInt32{Valid: false}
	}

	availabilityId := row[availabilityIdIndex]
	if availabilityId == "" {
		return sql.NullInt32{Valid: false}
	}

	if availabilities == nil {
		availabilities = make(map[string]int)
	}

	existentSequence, ok := availabilities[availabilityId]
	if !ok {
		availabilitySequence++
		availabilities[availabilityId] = availabilitySequence

		availabilityValues = append(
			availabilityValues,
			availabilitySequence,
			availabilityId,
		)

		return sql.NullInt32{
			Valid: true,
			Int32: int32(availabilitySequence),
		}
	}

	return sql.NullInt32{
		Valid: true,
		Int32: int32(existentSequence),
	}
}

func getAvailabilityStm() string {
	table := "dim_availabilities"
	cols := []string{
		"availability_sk",
		"availability_id",
	}
	totalVals := len(availabilityValues)
	return buildBatchInsert(table, cols, totalVals)
}

func resetAvailabilityValues() {
	availabilityValues = nil
}
