package importer

import "database/sql"

var (
	publisherSequence = 0
	publishers        map[string]int
	publisherValues   []any
)

func getPublisherSk(row []string) sql.NullInt32 {
	if len(row) <= publisherNameIndex {
		return sql.NullInt32{Valid: false}
	}

	publisherId := row[publisherIdIndex]
	publisherName := row[publisherNameIndex]

	if publisherId == "" {
		return sql.NullInt32{Valid: false}
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

		return sql.NullInt32{
			Valid: true,
			Int32: int32(publisherSequence),
		}
	}

	return sql.NullInt32{
		Valid: true,
		Int32: int32(existentSequence),
	}
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
