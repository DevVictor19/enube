package importer

var (
	chargeValues []any
)

func consumeChargeFacts(row []string) {
	usageDate := row[usageDateIndex]
	resourceUri := row[resourceUriIndex]
	effectiveUnitPrice := row[effectiveUnitPriceIndex]
	unitPrice := row[unitPriceIndex]
	quantity := row[quantityIndex]
	billingPreTaxTotal := row[billingPreTaxTotalIndex]
	billingCurrency := row[billingCurrencyIndex]
	pricingPreTaxTotal := row[pricingPreTaxTotalIndex]
	pricingCurrency := row[pricingCurrencyIndex]
	pcToBcExchangeRate := row[pcToBcExchangeRateIndex]
	pcToBcExchangeRateDate := row[pcToBcExchangeRateDateIndex]
	serviceInfo1 := row[serviceInfo1Index]
	serviceInfo2 := row[serviceInfo2Index]
	tags := row[tagsIndex]
	additionalInfo := row[additionalInfoIndex]

	chargeValues = append(chargeValues,
		getPartnerSk(row),
		getMonthsChargeDateSk(row),
		getCustomerSk(row),
		getMeterSk(row),
		getProductSk(row),
		getSkuSk(row),
		getPublisherSk(row),
		getSubscriptionSk(row),
		getResourceLocationSk(row),
		getResourceGroupSk(row),
		getServiceSk(row),
		getChargeTypeSk(row),
		getUnitTypeSk(row),
		getEntitlementSk(row),
		getPartnerSk(row),
		getBenefitSk(row),
		getBenefitOrderSk(row),
		getAvailabilitySk(row),
		toNullableDate(usageDate),
		resourceUri,
		toNullableFloat64(effectiveUnitPrice),
		toNullableFloat64(unitPrice),
		toNullableFloat64(quantity),
		toNullableFloat64(billingPreTaxTotal),
		billingCurrency,
		toNullableFloat64(pricingPreTaxTotal),
		pricingCurrency,
		toNullableFloat64(pcToBcExchangeRate),
		toNullableDate(pcToBcExchangeRateDate),
		serviceInfo1,
		serviceInfo2,
		tags,
		additionalInfo,
	)
}

func getFactChargesStm() string {
	table := "fact_charges"
	cols := []string{
		"partner_sk",
		"months_charge_date_sk",
		"customer_sk",
		"meter_sk",
		"product_sk",
		"sku_sk",
		"publisher_sk",
		"subscription_sk",
		"resource_location_sk",
		"resource_group_sk",
		"service_sk",
		"charge_type_sk",
		"unit_type_sk",
		"entitlement_sk",
		"partner_credit_sk",
		"benefit_sk",
		"benefit_order_sk",
		"availability_sk",
		"usage_date",
		"resource_uri",
		"effective_unit_price",
		"unit_price",
		"quantity",
		"billing_pre_tax_total",
		"billing_currency",
		"pricing_pre_tax_total",
		"pricing_currency",
		"pc_to_bc_exchange_rate",
		"pc_to_bc_exchange_rate_date",
		"service_info_1",
		"service_info_2",
		"tags",
		"additional_info",
	}
	totalVals := len(chargeValues)
	return buildBatchInsert(table, cols, totalVals)
}

func resetChargeValues() {
	chargeValues = nil
}
