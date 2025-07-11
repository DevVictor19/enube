package importer

var (
	availabilitySequence = 0
	availabilities       map[string]int
	availabilityValues   []any
)

func getAvailabilitySk(row []string) *int {
	if availabilities == nil {
		availabilities = make(map[string]int)
	}

	availabilityId := row[availabilityIdIndex]

	if availabilityId == "" {
		return nil
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

		return &availabilitySequence
	}

	return &existentSequence
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
