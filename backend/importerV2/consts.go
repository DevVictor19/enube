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

const (
	availabilitiesTable    string = "dim_availabilities"
	benefitOrdersTable     string = "dim_benefit_orders"
	benefitsTable          string = "dim_benefits"
	billingCurrenciesTable string = "dim_billing_currencies"
	chargeTypesTable       string = "dim_charge_types"
	customersTable         string = "dim_customers"
	entitlementsTable      string = "dim_entitlements"
	metersTable            string = "dim_meters"
	monthsChargeDatesTable string = "dim_months_charge_dates"
	partnerCreditsTable    string = "dim_partner_credits"
	partnersTable          string = "dim_partners"
	pricingCurrenciesTable string = "dim_pricing_currencies"
	productsTable          string = "dim_products"
	publishersTable        string = "dim_publishers"
	resourceGroupsTable    string = "dim_resource_groups"
	resourceLocationsTable string = "dim_resource_locations"
	servicesTable          string = "dim_services"
	skusTable              string = "dim_skus"
	subscriptionsTable     string = "dim_subscriptions"
	unitTypesTable         string = "dim_unit_types"
	usageDatesTable        string = "dim_usage_dates"
	factChargesTable       string = "fact_charges"
)

const (
	availabilityInsertQR      string = `INSERT INTO dim_availabilities (availability_sk, availability_id) VALUES `
	benefitOrdersInsertQR     string = `INSERT INTO dim_benefit_orders (benefit_order_sk, benefit_order_id) VALUES `
	benefitsInsertQR          string = `INSERT INTO dim_benefits (benefit_sk, benefit_id, type) VALUES `
	billingCurrenciesInsertQR string = `INSERT INTO dim_billing_currencies (billing_currency_sk, currency) VALUES `
	chargeTypesInsertQR       string = `INSERT INTO dim_charge_types (charge_type_sk, type) VALUES `
	customerInsertQR          string = `INSERT INTO dim_customers (customer_sk, customer_id, customer_name, customer_domain_name, customer_country, tier_2_mpn_id) VALUES `
	entitlementsInsertQR      string = `INSERT INTO dim_entitlements (entitlement_sk, entitlement_id, description) VALUES `
	metersInsertQR            string = `INSERT INTO dim_meters (meter_sk, meter_id, name, category, type, subcategory, region, unit) VALUES `
	monthsChargeDatesInsertQR string = `INSERT INTO dim_months_charge_dates (months_charge_date_sk, charge_start_date, charge_end_date) VALUES `
	partnerCreditsInsertQR    string = `INSERT INTO dim_partner_credits (partner_credit_sk, type, percentage, partner_earned_percentage) VALUES `
	partnersInsertQR          string = `INSERT INTO dim_partners (partner_sk, partner_id, partner_name, mpn_id, invoice_number) VALUES `
	pricingCurrenciesInsertQR string = `INSERT INTO dim_pricing_currencies (pricing_currency_sk, currency) VALUES `
	productsInsertQR          string = `INSERT INTO dim_products (product_sk, product_id, product_name) VALUES `
	publishersInsertQR        string = `INSERT INTO dim_publishers (publisher_sk, publisher_id, publisher_name) VALUES `
	resourceGroupsInsertQR    string = `INSERT INTO dim_resource_groups (resource_group_sk, name) VALUES `
	resourceLocationsInsertQR string = `INSERT INTO dim_resource_locations (resource_location_sk, location) VALUES `
	servicesInsertQR          string = `INSERT INTO dim_services (service_sk, service) VALUES `
	skusInsertQR              string = `INSERT INTO dim_skus (sku_sk, sku_id, sku_name) VALUES `
	subscriptionsInsertQR     string = `INSERT INTO dim_subscriptions (subscription_sk, subscription_id, description) VALUES `
	unitTypesInsertQR         string = `INSERT INTO dim_unit_types (unit_type_sk, type) VALUES `
	usageDatesInsertQR        string = `INSERT INTO dim_usage_dates (usage_date_sk, usage_date) VALUES `
	factChargesInsertQR       string = `INSERT INTO fact_charges (partner_sk, months_charge_date_sk, customer_sk, meter_sk, product_sk, sku_sk, publisher_sk, subscription_sk, resource_location_sk, resource_group_sk, service_sk, charge_type_sk, unit_type_sk, entitlement_sk, partner_credit_sk, benefit_sk, benefit_order_sk, availability_sk, usage_date_sk, billing_currency_sk, pricing_currency_sk, resource_uri, effective_unit_price, unit_price, quantity, billing_pre_tax_total, pricing_pre_tax_total, pc_to_bc_exchange_rate, pc_to_bc_exchange_rate_date, service_info_1, service_info_2, tags, additional_info) VALUES `
)

var tableCols = map[string]int{
	availabilitiesTable:    2,
	benefitOrdersTable:     2,
	benefitsTable:          3,
	billingCurrenciesTable: 2,
	chargeTypesTable:       2,
	customersTable:         6,
	entitlementsTable:      3,
	metersTable:            8,
	monthsChargeDatesTable: 3,
	partnerCreditsTable:    4,
	partnersTable:          5,
	pricingCurrenciesTable: 2,
	productsTable:          3,
	publishersTable:        3,
	resourceGroupsTable:    2,
	resourceLocationsTable: 2,
	servicesTable:          2,
	skusTable:              3,
	subscriptionsTable:     3,
	unitTypesTable:         2,
	usageDatesTable:        2,
	factChargesTable:       33,
}

var insertQueries = map[string]string{
	availabilitiesTable:    availabilityInsertQR,
	benefitOrdersTable:     benefitOrdersInsertQR,
	benefitsTable:          benefitsInsertQR,
	billingCurrenciesTable: billingCurrenciesInsertQR,
	chargeTypesTable:       chargeTypesInsertQR,
	customersTable:         customerInsertQR,
	entitlementsTable:      entitlementsInsertQR,
	metersTable:            metersInsertQR,
	monthsChargeDatesTable: monthsChargeDatesInsertQR,
	partnerCreditsTable:    partnerCreditsInsertQR,
	partnersTable:          partnersInsertQR,
	pricingCurrenciesTable: pricingCurrenciesInsertQR,
	productsTable:          productsInsertQR,
	publishersTable:        publishersInsertQR,
	resourceGroupsTable:    resourceGroupsInsertQR,
	resourceLocationsTable: resourceLocationsInsertQR,
	servicesTable:          servicesInsertQR,
	skusTable:              skusInsertQR,
	subscriptionsTable:     subscriptionsInsertQR,
	unitTypesTable:         unitTypesInsertQR,
	usageDatesTable:        usageDatesInsertQR,
	factChargesTable:       factChargesInsertQR,
}
