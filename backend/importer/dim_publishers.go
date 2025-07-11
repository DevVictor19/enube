package importer

var (
	publisherSequence = 0
	publishers        map[string]int
	publisherValues   []any
)

func getPublisherSk(row []string) *int {
	publisherId := row[publisherIdIndex]
	publisherName := row[publisherNameIndex]

	if publisherId == "" {
		return nil
	}

	if publishers == nil {
		publishers = make(map[string]int)
	}

	existentSequence, ok := publishers[publisherId]
	if !ok {
		publisherSequence++
		publishers[publisherId] = publisherSequence

		publisherValues = append(
			publisherValues,
			publisherSequence,
			publisherId,
			publisherName,
		)

		return &publisherSequence
	}

	return &existentSequence
}

func getPublisherStm() string {
	table := "dim_publishers"
	cols := []string{
		"publisher_sk",
		"publisher_id",
		"publisher_name",
	}
	totalVals := len(publisherValues)
	return buildBatchInsert(table, cols, totalVals)
}

func resetPublisherValues() {
	publisherValues = nil
}
