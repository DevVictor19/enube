package importerV2

import (
	"database/sql"
	"fmt"

	"github.com/DevVictor19/enube/backend/importerV2/helpers"
)

const availabilityInsertQR string = `INSERT INTO dim_availabilities (
    availability_sk,
    availability_id
)
VALUES `

func getAvailability(row []string) (sql.NullInt32, []any) {
	if len(row) <= availabilityIdIndex {
		return sql.NullInt32{Valid: false}, nil
	}

	availabilityId := row[availabilityIdIndex]
	if availabilityId == "" {
		return sql.NullInt32{Valid: false}, nil
	}

	seq, new := availabilitiesCache.NewEntry(availabilityId)
	if new {
		values := []any{
			seq,
			availabilityId,
		}

		return sql.NullInt32{Valid: true, Int32: seq}, values
	}

	return sql.NullInt32{Valid: true, Int32: seq}, nil
}

const benefitOrdersInsertQR = `INSERT INTO dim_benefit_orders (
    benefit_order_sk,
    benefit_order_id
)
VALUES `

func getBenefitOrders(row []string) (sql.NullInt32, []any) {
	if len(row) < 53 {
		return sql.NullInt32{
			Valid: false,
		}, nil
	}

	benefitOrderId := row[benefitOrderIdIndex]
	if benefitOrderId == "" {
		return sql.NullInt32{
			Valid: false,
		}, nil
	}

	seq, new := availabilitiesCache.NewEntry(benefitOrderId)
	if new {
		values := []any{
			seq,
			benefitOrderId,
		}

		return sql.NullInt32{Valid: true, Int32: seq}, values
	}

	return sql.NullInt32{Valid: true, Int32: seq}, nil
}

const benefitsInsertQR string = `INSERT INTO dim_benefits (
    benefit_sk,
    benefit_id,
    type
)
VALUES `

func getBenefits(row []string) (sql.NullInt32, []any) {
	if len(row) <= benefitTypeIndex {
		return sql.NullInt32{Valid: false}, nil
	}

	benefitId := row[benefitIdIndex]
	benefitType := row[benefitTypeIndex]

	if benefitId == "" {
		return sql.NullInt32{Valid: false}, nil
	}

	seq, new := availabilitiesCache.NewEntry(benefitId)
	if new {
		values := []any{
			seq,
			benefitId,
			benefitType,
		}

		return sql.NullInt32{Valid: true, Int32: seq}, values
	}

	return sql.NullInt32{Valid: true, Int32: seq}, nil
}

const billingCurrenciesInsertQR = `INSERT INTO dim_billing_currencies (
    billing_currency_sk,
    currency
)
VALUES `

func getBillingCurrencies(row []string) (sql.NullInt32, []any) {
	billingCurrency := row[billingCurrencyIndex]

	if billingCurrency == "" {
		return sql.NullInt32{Valid: false}, nil
	}

	seq, new := availabilitiesCache.NewEntry(billingCurrency)
	if new {
		values := []any{
			seq,
			billingCurrency,
		}

		return sql.NullInt32{Valid: true, Int32: seq}, values
	}

	return sql.NullInt32{Valid: true, Int32: seq}, nil
}

const chargeTypesInsertQR = `INSERT INTO dim_charge_types (
    charge_type_sk,
    type
)
VALUES `

func getChargeTypes(row []string) (sql.NullInt32, []any) {
	if len(row) <= chargeTypeIndex {
		return sql.NullInt32{Valid: false}, nil
	}

	chargeType := row[chargeTypeIndex]
	if chargeType == "" {
		return sql.NullInt32{Valid: false}, nil
	}

	seq, new := availabilitiesCache.NewEntry(chargeType)
	if new {
		values := []any{
			seq,
			chargeType,
		}

		return sql.NullInt32{Valid: true, Int32: seq}, values
	}

	return sql.NullInt32{Valid: true, Int32: seq}, nil
}

const customerInsertQR string = `INSERT INTO dim_customers (
    customer_sk,
    customer_id,
    customer_name,
    customer_domain_name,
    customer_country,
    tier_2_mpn_id
)
VALUES `

func getCustomers(row []string) (sql.NullInt32, []any) {
	if len(row) <= tier2MpnIdIndex {
		return sql.NullInt32{Valid: false}, nil
	}

	customerId := row[customerIdIndex]
	if customerId == "" {
		return sql.NullInt32{Valid: false}, nil
	}

	seq, new := customersCache.NewEntry(customerId)
	if new {
		customerName := row[customerNameIndex]
		customerDomain := row[customerDomainNameIndex]
		customerCountry := row[customerCountryIndex]
		tier2MpnId := row[tier2MpnIdIndex]

		values := []any{
			seq,
			customerId,
			customerName,
			customerDomain,
			customerCountry,
			helpers.ToNullableInt64(tier2MpnId),
		}

		return sql.NullInt32{Valid: true, Int32: seq}, values
	}

	return sql.NullInt32{Valid: true, Int32: seq}, nil
}

const entitlementsInsertQR = `INSERT INTO dim_entitlements (
    entitlement_sk,
    entitlement_id,
    description
)
VALUES `

func getEntitlements(row []string) (sql.NullInt32, []any) {
	if len(row) <= entitlementDescriptionIndex {
		return sql.NullInt32{Valid: false}, nil
	}

	entitlementId := row[entitlementIdIndex]
	description := row[entitlementDescriptionIndex]

	if entitlementId == "" {
		return sql.NullInt32{Valid: false}, nil
	}

	seq, new := availabilitiesCache.NewEntry(entitlementId)
	if new {
		values := []any{
			seq,
			entitlementId,
			description,
		}

		return sql.NullInt32{Valid: true, Int32: seq}, values
	}

	return sql.NullInt32{Valid: true, Int32: seq}, nil
}

const metersInsertQR = `INSERT INTO dim_meters (
    meter_sk,
    meter_id,
    name,
    category,
    type,
    subcategory,
    region,
    unit
)
VALUES `

func getMeters(row []string) (sql.NullInt32, []any) {
	if len(row) <= meterUnitIndex {
		return sql.NullInt32{Valid: false}, nil
	}

	meterId := row[meterIdIndex]
	name := row[meterNameIndex]
	category := row[meterCategoryIndex]
	meterType := row[meterTypeIndex]
	subcategory := row[meterSubCategoryIndex]
	region := row[meterRegionIndex]
	unit := row[meterUnitIndex]

	if meterId == "" {
		return sql.NullInt32{Valid: false}, nil
	}

	seq, new := availabilitiesCache.NewEntry(meterId)
	if new {
		values := []any{
			seq,
			meterId,
			name,
			category,
			meterType,
			subcategory,
			region,
			unit,
		}

		return sql.NullInt32{Valid: true, Int32: seq}, values
	}

	return sql.NullInt32{Valid: true, Int32: seq}, nil
}

const monthsChargeDatesInsertQR = `INSERT INTO dim_months_charge_dates (
    months_charge_date_sk,
    charge_start_date,
    charge_end_date
)
VALUES `

func getMonthsChargeDates(row []string) (sql.NullInt32, []any) {
	if len(row) <= chargeEndDateIndex {
		return sql.NullInt32{Valid: false}, nil
	}

	chargeStartDate := row[chargeStartDateIndex]
	chargeEndDate := row[chargeEndDateIndex]

	if chargeStartDate == "" || chargeEndDate == "" {
		return sql.NullInt32{Valid: false}, nil
	}

	key := fmt.Sprintf("%s|%s", chargeStartDate, chargeEndDate)
	seq, new := availabilitiesCache.NewEntry(key)
	if new {
		values := []any{
			seq,
			helpers.ToNullableDate(chargeStartDate),
			helpers.ToNullableDate(chargeEndDate),
		}
		return sql.NullInt32{Valid: true, Int32: seq}, values
	}

	return sql.NullInt32{Valid: true, Int32: seq}, nil
}

const partnerCreditsInsertQR = `INSERT INTO dim_partner_credits (
    partner_credit_sk,
    type,
    percentage,
    partner_earned_percentage
)
VALUES `

func getPartnerCredits(row []string) (sql.NullInt32, []any) {
	if len(row) <= partnerEarnedCreditPercentageIndex {
		return sql.NullInt32{Valid: false}, nil
	}

	creditType := row[creditTypeIndex]
	percentage := row[creditPercentageIndex]
	partnerEarnedPercentage := row[partnerEarnedCreditPercentageIndex]

	if creditType == "" {
		return sql.NullInt32{Valid: false}, nil
	}

	seq, new := availabilitiesCache.NewEntry(creditType)
	if new {
		values := []any{
			seq,
			creditType,
			helpers.ToNullableFloat64(percentage),
			helpers.ToNullableFloat64(partnerEarnedPercentage),
		}
		return sql.NullInt32{Valid: true, Int32: seq}, values
	}

	return sql.NullInt32{Valid: true, Int32: seq}, nil
}

const partnersInsertQR = `INSERT INTO dim_partners (
    partner_sk,
    partner_id,
    partner_name,
    mpn_id,
    invoice_number
)
VALUES `

func getPartners(row []string) (sql.NullInt32, []any) {
	if len(row) <= invoiceNumberIndex {
		return sql.NullInt32{Valid: false}, nil
	}

	partnerId := row[partnerIdIndex]
	partnerName := row[partnerNameIndex]
	mpnId := row[mpnIdIndex]
	invoiceNumber := row[invoiceNumberIndex]

	if partnerId == "" {
		return sql.NullInt32{Valid: false}, nil
	}

	seq, new := availabilitiesCache.NewEntry(partnerId)
	if new {
		values := []any{
			seq,
			partnerId,
			partnerName,
			helpers.ToNullableInt64(mpnId),
			invoiceNumber,
		}
		return sql.NullInt32{Valid: true, Int32: seq}, values
	}

	return sql.NullInt32{Valid: true, Int32: seq}, nil
}

const pricingCurrenciesInsertQR = `INSERT INTO dim_pricing_currencies (
    pricing_currency_sk,
    currency
)
VALUES `

func getPricingCurrencies(row []string) (sql.NullInt32, []any) {
	pricingCurrency := row[pricingCurrencyIndex]

	if pricingCurrency == "" {
		return sql.NullInt32{Valid: false}, nil
	}

	seq, new := availabilitiesCache.NewEntry(pricingCurrency)
	if new {
		values := []any{
			seq,
			pricingCurrency,
		}
		return sql.NullInt32{Valid: true, Int32: seq}, values
	}

	return sql.NullInt32{Valid: true, Int32: seq}, nil
}

const productsInsertQR = `INSERT INTO dim_products (
    product_sk,
    product_id,
    product_name
)
VALUES `

func getProducts(row []string) (sql.NullInt32, []any) {
	if len(row) <= productNameIndex {
		return sql.NullInt32{Valid: false}, nil
	}

	productId := row[productIdIndex]
	productName := row[productNameIndex]

	if productId == "" {
		return sql.NullInt32{Valid: false}, nil
	}

	seq, new := availabilitiesCache.NewEntry(productId)
	if new {
		values := []any{
			seq,
			productId,
			productName,
		}
		return sql.NullInt32{Valid: true, Int32: seq}, values
	}

	return sql.NullInt32{Valid: true, Int32: seq}, nil
}

const publishersInsertQR = `INSERT INTO dim_publishers (
    publisher_sk,
    publisher_id,
    publisher_name
)
VALUES `

func getPublishers(row []string) (sql.NullInt32, []any) {
	if len(row) <= publisherNameIndex {
		return sql.NullInt32{Valid: false}, nil
	}

	publisherId := row[publisherIdIndex]
	publisherName := row[publisherNameIndex]

	if publisherId == "" {
		return sql.NullInt32{Valid: false}, nil
	}

	seq, new := availabilitiesCache.NewEntry(publisherId)
	if new {
		values := []any{
			seq,
			publisherId,
			publisherName,
		}
		return sql.NullInt32{Valid: true, Int32: seq}, values
	}

	return sql.NullInt32{Valid: true, Int32: seq}, nil
}

const resourceGroupsInsertQR = `INSERT INTO dim_resource_groups (
    resource_group_sk,
    name
)
VALUES `

func getResourceGroups(row []string) (sql.NullInt32, []any) {
	if len(row) <= resourceGroupIndex {
		return sql.NullInt32{Valid: false}, nil
	}

	name := row[resourceGroupIndex]
	if name == "" {
		return sql.NullInt32{Valid: false}, nil
	}

	seq, new := availabilitiesCache.NewEntry(name)
	if new {
		values := []any{
			seq,
			name,
		}
		return sql.NullInt32{Valid: true, Int32: seq}, values
	}

	return sql.NullInt32{Valid: true, Int32: seq}, nil
}

const resourceLocationsInsertQR = `INSERT INTO dim_resource_locations (
    resource_location_sk,
    location
)
VALUES `

func getResourceLocations(row []string) (sql.NullInt32, []any) {
	if len(row) <= resourceLocationIndex {
		return sql.NullInt32{Valid: false}, nil
	}

	location := row[resourceLocationIndex]
	if location == "" {
		return sql.NullInt32{Valid: false}, nil
	}

	seq, new := availabilitiesCache.NewEntry(location)
	if new {
		values := []any{
			seq,
			location,
		}
		return sql.NullInt32{Valid: true, Int32: seq}, values
	}

	return sql.NullInt32{Valid: true, Int32: seq}, nil
}

const servicesInsertQR = `INSERT INTO dim_services (
    service_sk,
    service
)
VALUES `

func getServices(row []string) (sql.NullInt32, []any) {
	if len(row) <= consumedServiceIndex {
		return sql.NullInt32{Valid: false}, nil
	}

	service := row[consumedServiceIndex]
	if service == "" {
		return sql.NullInt32{Valid: false}, nil
	}

	seq, new := availabilitiesCache.NewEntry(service)
	if new {
		values := []any{
			seq,
			service,
		}
		return sql.NullInt32{Valid: true, Int32: seq}, values
	}

	return sql.NullInt32{Valid: true, Int32: seq}, nil
}

const skusInsertQR = `INSERT INTO dim_skus (
    sku_sk,
    sku_id,
    sku_name
)
VALUES `

func getSkus(row []string) (sql.NullInt32, []any) {
	if len(row) <= skuNameIndex {
		return sql.NullInt32{Valid: false}, nil
	}

	skuId := row[skuIdIndex]
	skuName := row[skuNameIndex]

	if skuId == "" {
		return sql.NullInt32{Valid: false}, nil
	}

	seq, new := availabilitiesCache.NewEntry(skuId)
	if new {
		values := []any{
			seq,
			skuId,
			skuName,
		}
		return sql.NullInt32{Valid: true, Int32: seq}, values
	}

	return sql.NullInt32{Valid: true, Int32: seq}, nil
}

const subscriptionsInsertQR = `INSERT INTO dim_subscriptions (
    subscription_sk,
    subscription_id,
    description
)
VALUES `

func getSubscriptions(row []string) (sql.NullInt32, []any) {
	subscriptionId := row[subscriptionIdIndex]
	description := row[subscriptionDescriptionIndex]

	if subscriptionId == "" {
		return sql.NullInt32{Valid: false}, nil
	}

	seq, new := availabilitiesCache.NewEntry(subscriptionId)
	if new {
		values := []any{
			seq,
			subscriptionId,
			description,
		}
		return sql.NullInt32{Valid: true, Int32: seq}, values
	}

	return sql.NullInt32{Valid: true, Int32: seq}, nil
}

const unitTypesInsertQR = `INSERT INTO dim_unit_types (
    unit_type_sk,
    type
)
VALUES `

func getUnitTypes(row []string) (sql.NullInt32, []any) {
	unitType := row[unitTypeIndex]

	if unitType == "" {
		return sql.NullInt32{Valid: false}, nil
	}

	seq, new := availabilitiesCache.NewEntry(unitType)
	if new {
		values := []any{
			seq,
			unitType,
		}
		return sql.NullInt32{Valid: true, Int32: seq}, values
	}

	return sql.NullInt32{Valid: true, Int32: seq}, nil
}

const usageDatesInsertQR = `INSERT INTO dim_usage_dates (
    usage_date_sk,
    usage_date
)
VALUES `

func getUsageDates(row []string) (sql.NullInt32, []any) {
	usageDate := row[usageDateIndex]

	if usageDate == "" {
		return sql.NullInt32{Valid: false}, nil
	}

	seq, new := availabilitiesCache.NewEntry(usageDate)
	if new {
		values := []any{
			seq,
			helpers.ToNullableDate(usageDate),
		}
		return sql.NullInt32{Valid: true, Int32: seq}, values
	}

	return sql.NullInt32{Valid: true, Int32: seq}, nil
}

const factChargesInsertQR string = `INSERT INTO act_charges (
    partner_sk,
    months_charge_date_sk,
    customer_sk,
    meter_sk,
    product_sk,
    sku_sk,
    publisher_sk,
    subscription_sk,
    resource_location_sk,
    resource_group_sk,
    service_sk,
    charge_type_sk,
    unit_type_sk,
    entitlement_sk,
    partner_credit_sk,
    benefit_sk,
    benefit_order_sk,
    availability_sk,
    usage_date_sk,
    billing_currency_sk,
    pricing_currency_sk,
    resource_uri,
    effective_unit_price,
    unit_price,
    quantity,
    billing_pre_tax_total,
    pricing_pre_tax_total,
    pc_to_bc_exchange_rate,
    pc_to_bc_exchange_rate_date,
    service_info_1,
    service_info_2,
    tags,
    additional_info
)
VALUES `
