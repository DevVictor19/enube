package importer

var (
	skuSequence = 0
	skus        map[string]int
	skuValues   []any
)

func getSkuSk(row []string) int {
	if skus == nil {
		skus = make(map[string]int)
	}

	skuId := row[skuIdIndex]
	skuName := row[skuNameIndex]

	if skuId == "" {
		return 0
	}

	existentSequence, ok := skus[skuId]
	if !ok {
		skuSequence++
		skus[skuId] = skuSequence

		skuValues = append(
			skuValues,
			skuSequence,
			skuId,
			skuName,
		)

		return skuSequence
	}

	return existentSequence
}

func getSkuStm() string {
	table := "dim_skus"
	cols := []string{
		"sku_sk",
		"sku_id",
		"sku_name",
	}
	totalVals := len(skuValues)
	return buildBatchInsert(table, cols, totalVals)
}

func resetSkuValues() {
	skuValues = nil
}
