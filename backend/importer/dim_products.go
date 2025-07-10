package importer

var (
	productSequence = 0
	products        map[string]int
	productValues   []any
)

func getProductSk(row []string) int {
	if products == nil {
		products = make(map[string]int)
	}

	productId := row[productIdIndex]
	productName := row[productNameIndex]

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

		return productSequence
	}

	return existentSequence
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
