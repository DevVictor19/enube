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

type tableName string

const (
	availabilitiesTable    tableName = "dim_availabilities"
	benefitOrdersTable     tableName = "dim_benefit_orders"
	benefitsTable          tableName = "dim_benefits"
	billingCurrenciesTable tableName = "dim_billing_currencies"
	chargeTypesTable       tableName = "dim_charge_types"
	customersTable         tableName = "dim_customers"
	entitlementsTable      tableName = "dim_entitlements"
	metersTable            tableName = "dim_meters"
	monthsChargeDatesTable tableName = "dim_months_charge_dates"
	partnerCreditsTable    tableName = "dim_partner_credits"
	partnersTable          tableName = "dim_partners"
	pricingCurrenciesTable tableName = "dim_pricing_currencies"
	productsTable          tableName = "dim_products"
	publishersTable        tableName = "dim_publishers"
	resourceGroupsTable    tableName = "dim_resource_groups"
	resourceLocationsTable tableName = "dim_resource_locations"
	servicesTable          tableName = "dim_services"
	skusTable              tableName = "dim_skus"
	subscriptionsTable     tableName = "dim_subscriptions"
	unitTypesTable         tableName = "dim_unit_types"
	usageDatesTable        tableName = "dim_usage_dates"
	factChargesTable       tableName = "fact_charges"
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
