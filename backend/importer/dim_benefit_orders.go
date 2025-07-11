package importer

var (
	benefitOrderSequence = 0
	benefitOrders        map[string]int
	benefitOrderValues   []any
)

func getBenefitOrderSk(row []string) *int {
	if len(row) < 53 {
		return nil
	}

	benefitOrderId := row[benefitOrderIdIndex]

	if benefitOrderId == "" {
		return nil
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

		return &benefitOrderSequence
	}

	return &existentSequence
}

func getBenefitOrderStm() string {
	table := "dim_benefit_orders"
	cols := []string{
		"benefit_order_sk",
		"benefit_order_id",
	}
	totalVals := len(benefitOrderValues)
	return buildBatchInsert(table, cols, totalVals)
}

func resetBenefitOrderValues() {
	benefitOrderValues = nil
}
