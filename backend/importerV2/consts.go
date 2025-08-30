package importerV2

var (
	availabilitiesCache    tableCache
	benefitOrdersCache     tableCache
	benefitsCache          tableCache
	billingCurrenciesCache tableCache
	chargeTypesCache       tableCache
	customersCache         tableCache
	entitlementsCache      tableCache
	metersCache            tableCache
	monthsChargeDatesCache tableCache
	partnerCreditsCache    tableCache
	partnersCache          tableCache
	pricingCurrenciesCache tableCache
	productsCache          tableCache
	publishersCache        tableCache
	resourceGroupsCache    tableCache
	resourceLocationsCache tableCache
	servicesCache          tableCache
	skusCache              tableCache
	subscriptionsCache     tableCache
	unitTypesCache         tableCache
	usageDatesCache        tableCache
)

const (
	partnerIdIndex int = iota
	partnerNameIndex
	customerIdIndex
	customerNameIndex
	customerDomainNameIndex
	customerCountryIndex
	mpnIdIndex
	tier2MpnIdIndex
	invoiceNumberIndex
	productIdIndex
	skuIdIndex
	availabilityIdIndex
	skuNameIndex
	productNameIndex
	publisherNameIndex
	publisherIdIndex
	subscriptionDescriptionIndex
	subscriptionIdIndex
	chargeStartDateIndex
	chargeEndDateIndex
	usageDateIndex
	meterTypeIndex
	meterCategoryIndex
	meterIdIndex
	meterSubCategoryIndex
	meterNameIndex
	meterRegionIndex
	meterUnitIndex
	resourceLocationIndex
	consumedServiceIndex
	resourceGroupIndex
	resourceUriIndex
	chargeTypeIndex
	unitPriceIndex
	quantityIndex
	unitTypeIndex
	billingPreTaxTotalIndex
	billingCurrencyIndex
	pricingPreTaxTotalIndex
	pricingCurrencyIndex
	serviceInfo1Index
	serviceInfo2Index
	tagsIndex
	additionalInfoIndex
	effectiveUnitPriceIndex
	pcToBcExchangeRateIndex
	pcToBcExchangeRateDateIndex
	entitlementIdIndex
	entitlementDescriptionIndex
	partnerEarnedCreditPercentageIndex
	creditPercentageIndex
	creditTypeIndex
	benefitOrderIdIndex
	benefitIdIndex
	benefitTypeIndex
)
