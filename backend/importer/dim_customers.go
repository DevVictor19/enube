package importer

var (
	customerSequence = 0
	customers        map[string]int
	customerValues   []any
)

func getCustomerSk(row []string) *int {
	customerId := row[customerIdIndex]
	customerName := row[customerNameIndex]
	customerDomain := row[customerDomainNameIndex]
	customerCountry := row[customerCountryIndex]
	tier2MpnId := row[tier2MpnIdIndex]

	if customerId == "" {
		return nil
	}

	if customers == nil {
		customers = make(map[string]int)
	}

	existentSequence, ok := customers[customerId]
	if !ok {
		customerSequence++
		customers[customerId] = customerSequence

		customerValues = append(
			customerValues,
			customerSequence,
			customerId,
			customerName,
			customerDomain,
			customerCountry,
			toNullableInt64(tier2MpnId),
		)

		return &customerSequence
	}

	return &existentSequence
}

func getCustomerStm() string {
	table := "dim_customers"
	cols := []string{
		"customer_sk",
		"customer_id",
		"customer_name",
		"customer_domain_name",
		"customer_country",
		"tier_2_mpn_id",
	}
	totalVals := len(customerValues)
	return buildBatchInsert(table, cols, totalVals)
}

func resetCustomerValues() {
	customerValues = nil
}
