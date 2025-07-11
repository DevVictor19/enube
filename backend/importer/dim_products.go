package importer

import "database/sql"

var (
	productSequence = 0
	products        map[string]int
	productValues   []any
)

func getProductSk(row []string) sql.NullInt32 {
	if len(row) <= productNameIndex {
		return sql.NullInt32{Valid: false}
	}

	productId := row[productIdIndex]
	productName := row[productNameIndex]

	if productId == "" {
		return sql.NullInt32{Valid: false}
	}

	if products == nil {
		products = make(map[string]int)
	}

	existentSequence, ok := products[productId]
	if !ok {
		productSequence++
		products[productId] = productSequence

		productValues = append(
			productValues,
			productSequence,
			productId,
			productName,
		)

		return sql.NullInt32{
			Valid: true,
			Int32: int32(productSequence),
		}
	}

	return sql.NullInt32{
		Valid: true,
		Int32: int32(existentSequence),
	}
}

func getProductStm() string {
	table := "dim_products"
	cols := []string{
		"product_sk",
		"product_id",
		"product_name",
	}
	totalVals := len(productValues)
	return buildBatchInsert(table, cols, totalVals)
}

func resetProductValues() {
	productValues = nil
}
