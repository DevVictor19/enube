package importer

import (
	"database/sql"

	"github.com/DevVictor19/enube/backend/importer/helpers"
)

var (
	benefitOrderSequence = 0
	benefitOrders        map[string]int
	benefitOrderValues   []any
)

func getBenefitOrderSk(row []string) sql.NullInt32 {
	if len(row) < 53 {
		return sql.NullInt32{
			Valid: false,
		}
	}

	benefitOrderId := row[benefitOrderIdIndex]

	if benefitOrderId == "" {
		return sql.NullInt32{
			Valid: false,
		}
	}

	if benefitOrders == nil {
		benefitOrders = make(map[string]int)
	}

	existentSequence, ok := benefitOrders[benefitOrderId]
	if !ok {
		benefitOrderSequence++
		benefitOrders[benefitOrderId] = benefitOrderSequence

		benefitOrderValues = append(
			benefitOrderValues,
			benefitOrderSequence,
			benefitOrderId,
		)

		return sql.NullInt32{
			Valid: true,
			Int32: int32(benefitOrderSequence),
		}
	}

	return sql.NullInt32{
		Valid: true,
		Int32: int32(existentSequence),
	}
}

func getBenefitOrderStm() string {
	table := "dim_benefit_orders"
	cols := []string{
		"benefit_order_sk",
		"benefit_order_id",
	}
	totalVals := len(benefitOrderValues)
	return helpers.BuildBatchInsert(table, cols, totalVals)
}

func resetBenefitOrderValues() {
	benefitOrderValues = nil
}
