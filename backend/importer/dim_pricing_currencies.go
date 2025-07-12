package importer

import (
	"database/sql"

	"github.com/DevVictor19/enube/backend/importer/helpers"
)

var (
	pricingCurrencySequence = 0
	pricingCurrencies       map[string]int
	pricingCurrencyValues   []any
)

func getPricingCurrencySk(row []string) sql.NullInt32 {
	pricingCurrency := row[pricingCurrencyIndex]

	if pricingCurrency == "" {
		return sql.NullInt32{Valid: false}
	}

	if pricingCurrencies == nil {
		pricingCurrencies = make(map[string]int)
	}

	existentSequence, ok := pricingCurrencies[pricingCurrency]
	if !ok {
		pricingCurrencySequence++
		pricingCurrencies[pricingCurrency] = pricingCurrencySequence

		pricingCurrencyValues = append(
			pricingCurrencyValues,
			pricingCurrencySequence,
			pricingCurrency,
		)

		return sql.NullInt32{
			Valid: true,
			Int32: int32(pricingCurrencySequence),
		}
	}

	return sql.NullInt32{
		Valid: true,
		Int32: int32(existentSequence),
	}
}

func getPricingCurrencyStm() string {
	table := "dim_pricing_currencies"
	cols := []string{
		"pricing_currency_sk",
		"currency",
	}
	totalVals := len(pricingCurrencyValues)
	return helpers.BuildBatchInsert(table, cols, totalVals)
}

func resetPricingCurrencyValues() {
	pricingCurrencyValues = nil
}
