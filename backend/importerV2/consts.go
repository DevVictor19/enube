package importerV2

var (
	availabilitiesCache    *tableCache = newTableCache()
	benefitOrdersCache     *tableCache = newTableCache()
	benefitsCache          *tableCache = newTableCache()
	billingCurrenciesCache *tableCache = newTableCache()
	chargeTypesCache       *tableCache = newTableCache()
	customersCache         *tableCache = newTableCache()
	entitlementsCache      *tableCache = newTableCache()
	metersCache            *tableCache = newTableCache()
	monthsChargeDatesCache *tableCache = newTableCache()
	partnerCreditsCache    *tableCache = newTableCache()
	partnersCache          *tableCache = newTableCache()
	pricingCurrenciesCache *tableCache = newTableCache()
	productsCache          *tableCache = newTableCache()
	publishersCache        *tableCache = newTableCache()
	resourceGroupsCache    *tableCache = newTableCache()
	resourceLocationsCache *tableCache = newTableCache()
	servicesCache          *tableCache = newTableCache()
	skusCache              *tableCache = newTableCache()
	subscriptionsCache     *tableCache = newTableCache()
	unitTypesCache         *tableCache = newTableCache()
	usageDatesCache        *tableCache = newTableCache()
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
