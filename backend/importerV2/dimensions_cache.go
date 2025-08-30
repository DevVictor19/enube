package importerV2

import (
	"database/sql"
	"fmt"

	"github.com/DevVictor19/enube/backend/importerV2/helpers"
)

func getAvailabilitySK(row []string) sql.NullInt32 {
	if len(row) <= availabilityIdIndex {
		return sql.NullInt32{Valid: false}
	}

	availabilityId := row[availabilityIdIndex]
	if availabilityId == "" {
		return sql.NullInt32{Valid: false}
	}

	seq, new := availabilitiesCache.NewEntry(availabilityId)
	if new {
		valuesMap.availabilities = append(valuesMap.availabilities, seq, availabilityId)
		return sql.NullInt32{Valid: true, Int32: seq}
	}

	return sql.NullInt32{Valid: true, Int32: seq}
}

func getBenefitOrdersSK(row []string) sql.NullInt32 {
	if len(row) <= benefitOrderIdIndex {
		return sql.NullInt32{Valid: false}
	}

	benefitOrderId := row[benefitOrderIdIndex]
	if benefitOrderId == "" {
		return sql.NullInt32{Valid: false}
	}

	seq, new := benefitOrdersCache.NewEntry(benefitOrderId)
	if new {
		valuesMap.benefitOrders = append(valuesMap.benefitOrders, seq, benefitOrderId)
		return sql.NullInt32{Valid: true, Int32: seq}
	}

	return sql.NullInt32{Valid: true, Int32: seq}
}

func getBenefitsSK(row []string) sql.NullInt32 {
	if len(row) <= benefitTypeIndex {
		return sql.NullInt32{Valid: false}
	}

	benefitId := row[benefitIdIndex]
	benefitType := row[benefitTypeIndex]

	if benefitId == "" {
		return sql.NullInt32{Valid: false}
	}

	seq, new := benefitsCache.NewEntry(benefitId)
	if new {
		valuesMap.benefits = append(valuesMap.benefits, seq, benefitId, benefitType)
		return sql.NullInt32{Valid: true, Int32: seq}
	}

	return sql.NullInt32{Valid: true, Int32: seq}
}

func getBillingCurrenciesSK(row []string) sql.NullInt32 {
	if len(row) <= billingCurrencyIndex {
		return sql.NullInt32{Valid: false}
	}

	billingCurrency := row[billingCurrencyIndex]
	if billingCurrency == "" {
		return sql.NullInt32{Valid: false}
	}

	seq, new := billingCurrenciesCache.NewEntry(billingCurrency)
	if new {
		valuesMap.billingCurrencies = append(valuesMap.billingCurrencies, seq, billingCurrency)
		return sql.NullInt32{Valid: true, Int32: seq}
	}

	return sql.NullInt32{Valid: true, Int32: seq}
}

func getChargeTypesSK(row []string) sql.NullInt32 {
	if len(row) <= chargeTypeIndex {
		return sql.NullInt32{Valid: false}
	}

	chargeType := row[chargeTypeIndex]
	if chargeType == "" {
		return sql.NullInt32{Valid: false}
	}

	seq, new := chargeTypesCache.NewEntry(chargeType)
	if new {
		valuesMap.chargeTypes = append(valuesMap.chargeTypes, seq, chargeType)
		return sql.NullInt32{Valid: true, Int32: seq}
	}

	return sql.NullInt32{Valid: true, Int32: seq}
}

func getCustomersSK(row []string) sql.NullInt32 {
	if len(row) <= tier2MpnIdIndex {
		return sql.NullInt32{Valid: false}
	}

	customerId := row[customerIdIndex]
	if customerId == "" {
		return sql.NullInt32{Valid: false}
	}

	seq, new := customersCache.NewEntry(customerId)
	if new {
		customerName := row[customerNameIndex]
		customerDomain := row[customerDomainNameIndex]
		customerCountry := row[customerCountryIndex]
		tier2MpnId := row[tier2MpnIdIndex]

		valuesMap.customers = append(valuesMap.customers, seq,
			customerId,
			customerName,
			customerDomain,
			customerCountry,
			helpers.ToNullableInt64(tier2MpnId))

		return sql.NullInt32{Valid: true, Int32: seq}
	}

	return sql.NullInt32{Valid: true, Int32: seq}
}

func getEntitlementsSK(row []string) sql.NullInt32 {
	if len(row) <= entitlementDescriptionIndex {
		return sql.NullInt32{Valid: false}
	}

	entitlementId := row[entitlementIdIndex]
	description := row[entitlementDescriptionIndex]

	if entitlementId == "" {
		return sql.NullInt32{Valid: false}
	}

	seq, new := entitlementsCache.NewEntry(entitlementId)
	if new {
		valuesMap.entitlements = append(valuesMap.entitlements, seq, entitlementId, description)
		return sql.NullInt32{Valid: true, Int32: seq}
	}

	return sql.NullInt32{Valid: true, Int32: seq}
}

func getMetersSK(row []string) sql.NullInt32 {
	if len(row) <= meterUnitIndex {
		return sql.NullInt32{Valid: false}
	}

	meterId := row[meterIdIndex]
	name := row[meterNameIndex]
	category := row[meterCategoryIndex]
	meterType := row[meterTypeIndex]
	subcategory := row[meterSubCategoryIndex]
	region := row[meterRegionIndex]
	unit := row[meterUnitIndex]

	if meterId == "" {
		return sql.NullInt32{Valid: false}
	}

	seq, new := metersCache.NewEntry(meterId)
	if new {
		valuesMap.meters = append(valuesMap.meters, seq, meterId, name, category, meterType, subcategory, region, unit)
		return sql.NullInt32{Valid: true, Int32: seq}
	}

	return sql.NullInt32{Valid: true, Int32: seq}
}

func getMonthsChargeDatesSK(row []string) sql.NullInt32 {
	if len(row) <= chargeEndDateIndex {
		return sql.NullInt32{Valid: false}
	}

	chargeStartDate := row[chargeStartDateIndex]
	chargeEndDate := row[chargeEndDateIndex]

	if chargeStartDate == "" || chargeEndDate == "" {
		return sql.NullInt32{Valid: false}
	}

	key := fmt.Sprintf("%s|%s", chargeStartDate, chargeEndDate)
	seq, new := monthsChargeDatesCache.NewEntry(key)
	if new {
		valuesMap.monthsChargeDates = append(valuesMap.monthsChargeDates,
			seq,
			helpers.ToNullableDate(chargeStartDate),
			helpers.ToNullableDate(chargeEndDate),
		)
		return sql.NullInt32{Valid: true, Int32: seq}
	}

	return sql.NullInt32{Valid: true, Int32: seq}
}

func getPartnerCreditsSK(row []string) sql.NullInt32 {
	if len(row) <= partnerEarnedCreditPercentageIndex {
		return sql.NullInt32{Valid: false}
	}

	creditType := row[creditTypeIndex]
	percentage := row[creditPercentageIndex]
	partnerEarnedPercentage := row[partnerEarnedCreditPercentageIndex]

	if creditType == "" {
		return sql.NullInt32{Valid: false}
	}

	seq, new := partnerCreditsCache.NewEntry(creditType)
	if new {
		valuesMap.partnerCredits = append(valuesMap.partnerCredits,
			seq,
			creditType,
			helpers.ToNullableFloat64(percentage),
			helpers.ToNullableFloat64(partnerEarnedPercentage),
		)
		return sql.NullInt32{Valid: true, Int32: seq}
	}

	return sql.NullInt32{Valid: true, Int32: seq}
}

func getPartnersSK(row []string) sql.NullInt32 {
	if len(row) <= invoiceNumberIndex {
		return sql.NullInt32{Valid: false}
	}

	partnerId := row[partnerIdIndex]
	partnerName := row[partnerNameIndex]
	mpnId := row[mpnIdIndex]
	invoiceNumber := row[invoiceNumberIndex]

	if partnerId == "" {
		return sql.NullInt32{Valid: false}
	}

	seq, new := partnersCache.NewEntry(partnerId)
	if new {
		valuesMap.partners = append(valuesMap.partners,
			seq,
			partnerId,
			partnerName,
			helpers.ToNullableInt64(mpnId),
			invoiceNumber,
		)
		return sql.NullInt32{Valid: true, Int32: seq}
	}

	return sql.NullInt32{Valid: true, Int32: seq}
}

func getPricingCurrenciesSK(row []string) sql.NullInt32 {
	if len(row) <= pricingCurrencyIndex {
		return sql.NullInt32{Valid: false}
	}

	pricingCurrency := row[pricingCurrencyIndex]
	if pricingCurrency == "" {
		return sql.NullInt32{Valid: false}
	}

	seq, new := pricingCurrenciesCache.NewEntry(pricingCurrency)
	if new {
		valuesMap.pricingCurrencies = append(valuesMap.pricingCurrencies, seq, pricingCurrency)
		return sql.NullInt32{Valid: true, Int32: seq}
	}

	return sql.NullInt32{Valid: true, Int32: seq}
}

func getProductsSK(row []string) sql.NullInt32 {
	if len(row) <= productNameIndex {
		return sql.NullInt32{Valid: false}
	}

	productId := row[productIdIndex]
	productName := row[productNameIndex]

	if productId == "" {
		return sql.NullInt32{Valid: false}
	}

	seq, new := productsCache.NewEntry(productId)
	if new {
		valuesMap.products = append(valuesMap.products, seq, productId, productName)
		return sql.NullInt32{Valid: true, Int32: seq}
	}

	return sql.NullInt32{Valid: true, Int32: seq}
}

func getPublishersSK(row []string) sql.NullInt32 {
	if len(row) <= publisherNameIndex {
		return sql.NullInt32{Valid: false}
	}

	publisherId := row[publisherIdIndex]
	publisherName := row[publisherNameIndex]

	if publisherId == "" {
		return sql.NullInt32{Valid: false}
	}

	seq, new := publishersCache.NewEntry(publisherId)
	if new {
		valuesMap.publishers = append(valuesMap.publishers, seq, publisherId, publisherName)
		return sql.NullInt32{Valid: true, Int32: seq}
	}

	return sql.NullInt32{Valid: true, Int32: seq}
}

func getResourceGroupsSK(row []string) sql.NullInt32 {
	if len(row) <= resourceGroupIndex {
		return sql.NullInt32{Valid: false}
	}

	name := row[resourceGroupIndex]
	if name == "" {
		return sql.NullInt32{Valid: false}
	}

	seq, new := resourceGroupsCache.NewEntry(name)
	if new {
		valuesMap.resourceGroups = append(valuesMap.resourceGroups, seq, name)
		return sql.NullInt32{Valid: true, Int32: seq}
	}

	return sql.NullInt32{Valid: true, Int32: seq}
}

func getResourceLocationsSK(row []string) sql.NullInt32 {
	if len(row) <= resourceLocationIndex {
		return sql.NullInt32{Valid: false}
	}

	location := row[resourceLocationIndex]
	if location == "" {
		return sql.NullInt32{Valid: false}
	}

	seq, new := resourceLocationsCache.NewEntry(location)
	if new {
		valuesMap.resourceLocations = append(valuesMap.resourceLocations, seq, location)
		return sql.NullInt32{Valid: true, Int32: seq}
	}

	return sql.NullInt32{Valid: true, Int32: seq}
}

func getServicesSK(row []string) sql.NullInt32 {
	if len(row) <= consumedServiceIndex {
		return sql.NullInt32{Valid: false}
	}

	service := row[consumedServiceIndex]
	if service == "" {
		return sql.NullInt32{Valid: false}
	}

	seq, new := servicesCache.NewEntry(service)
	if new {
		valuesMap.services = append(valuesMap.services, seq, service)
		return sql.NullInt32{Valid: true, Int32: seq}
	}

	return sql.NullInt32{Valid: true, Int32: seq}
}

func getSkusSK(row []string) sql.NullInt32 {
	if len(row) <= skuNameIndex {
		return sql.NullInt32{Valid: false}
	}

	skuId := row[skuIdIndex]
	skuName := row[skuNameIndex]

	if skuId == "" {
		return sql.NullInt32{Valid: false}
	}

	seq, new := skusCache.NewEntry(skuId)
	if new {
		valuesMap.skus = append(valuesMap.skus, seq, skuId, skuName)
		return sql.NullInt32{Valid: true, Int32: seq}
	}

	return sql.NullInt32{Valid: true, Int32: seq}
}

func getSubscriptionsSK(row []string) sql.NullInt32 {
	subscriptionId := row[subscriptionIdIndex]
	description := row[subscriptionDescriptionIndex]

	if subscriptionId == "" {
		return sql.NullInt32{Valid: false}
	}

	seq, new := subscriptionsCache.NewEntry(subscriptionId)
	if new {
		valuesMap.subscriptions = append(valuesMap.subscriptions, seq, subscriptionId, description)
		return sql.NullInt32{Valid: true, Int32: seq}
	}

	return sql.NullInt32{Valid: true, Int32: seq}
}

func getUnitTypesSK(row []string) sql.NullInt32 {
	if len(row) <= unitTypeIndex {
		return sql.NullInt32{Valid: false}
	}

	unitType := row[unitTypeIndex]
	if unitType == "" {
		return sql.NullInt32{Valid: false}
	}

	seq, new := unitTypesCache.NewEntry(unitType)
	if new {
		valuesMap.unitTypes = append(valuesMap.unitTypes, seq, unitType)
		return sql.NullInt32{Valid: true, Int32: seq}
	}

	return sql.NullInt32{Valid: true, Int32: seq}
}

func getUsageDatesSK(row []string) sql.NullInt32 {
	if len(row) <= usageDateIndex {
		return sql.NullInt32{Valid: false}
	}

	usageDate := row[usageDateIndex]
	if usageDate == "" {
		return sql.NullInt32{Valid: false}
	}

	seq, new := usageDatesCache.NewEntry(usageDate)
	if new {
		valuesMap.usageDates = append(valuesMap.usageDates, seq, helpers.ToNullableDate(usageDate))
		return sql.NullInt32{Valid: true, Int32: seq}
	}

	return sql.NullInt32{Valid: true, Int32: seq}
}
