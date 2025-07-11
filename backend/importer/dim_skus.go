package importer

import (
	"database/sql"

	"github.com/DevVictor19/enube/backend/importer/helpers"
)

var (
	skuSequence = 0
	skus        map[string]int
	skuValues   []any
)

func getSkuSk(row []string) sql.NullInt32 {
	if len(row) <= skuNameIndex {
		return sql.NullInt32{Valid: false}
	}

	skuId := row[skuIdIndex]
	skuName := row[skuNameIndex]

	if skuId == "" {
		return sql.NullInt32{Valid: false}
	}

	if skus == nil {
		skus = make(map[string]int)
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

		return sql.NullInt32{
			Valid: true,
			Int32: int32(skuSequence),
		}
	}

	return sql.NullInt32{
		Valid: true,
		Int32: int32(existentSequence),
	}
}

func getSkuStm() string {
	table := "dim_skus"
	cols := []string{
		"sku_sk",
		"sku_id",
		"sku_name",
	}
	totalVals := len(skuValues)
	return helpers.BuildBatchInsert(table, cols, totalVals)
}

func resetSkuValues() {
	skuValues = nil
}
