package importer

import (
	"database/sql"

	"github.com/DevVictor19/enube/backend/importer/helpers"
)

var (
	billingCurrencySequence = 0
	billingCurrencies       map[string]int
	billingCurrencyValues   []any
)

func getBillingCurrencySk(row []string) sql.NullInt32 {
	billingCurrency := row[billingCurrencyIndex]

	if billingCurrency == "" {
		return sql.NullInt32{Valid: false}
	}

	if billingCurrencies == nil {
		billingCurrencies = make(map[string]int)
	}

	existentSequence, ok := billingCurrencies[billingCurrency]
	if !ok {
		billingCurrencySequence++
		billingCurrencies[billingCurrency] = billingCurrencySequence

		billingCurrencyValues = append(
			billingCurrencyValues,
			billingCurrencySequence,
			billingCurrency,
		)

		return sql.NullInt32{
			Valid: true,
			Int32: int32(billingCurrencySequence),
		}
	}

	return sql.NullInt32{
		Valid: true,
		Int32: int32(existentSequence),
	}
}

func getBillingCurrencyStm() string {
	table := "dim_billing_currencies"
	cols := []string{
		"billing_currency_sk",
		"currency",
	}
	totalVals := len(billingCurrencyValues)
	return helpers.BuildBatchInsert(table, cols, totalVals)
}

func resetBillingCurrencyValues() {
	billingCurrencyValues = nil
}
