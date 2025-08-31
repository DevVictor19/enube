package importerV2

import (
	"database/sql"
	"fmt"

	"github.com/DevVictor19/enube/backend/importerV2/helpers"
)

func getAvailabilitySK(row []string, values *[]any) sql.NullInt32 {
	if len(row) <= availabilityIdIndex {
		return sql.NullInt32{Valid: false}
	}

	availabilityId := row[availabilityIdIndex]
	if availabilityId == "" {
		return sql.NullInt32{Valid: false}
	}

	seq, found := availabilitiesCache.Get(availabilityId)
	if found {
		return sql.NullInt32{Valid: true, Int32: seq}
	}

	seq = availabilitiesCache.Set(availabilityId)
	*values = append(*values, seq, availabilityId)

	return sql.NullInt32{Valid: true, Int32: seq}
}

func getBenefitOrdersSK(row []string, values *[]any) sql.NullInt32 {
	if len(row) <= benefitOrderIdIndex {
		return sql.NullInt32{Valid: false}
	}

	benefitOrderId := row[benefitOrderIdIndex]
	if benefitOrderId == "" {
		return sql.NullInt32{Valid: false}
	}

	seq, found := benefitOrdersCache.Get(benefitOrderId)
	if found {
		return sql.NullInt32{Valid: true, Int32: seq}
	}

	seq = benefitOrdersCache.Set(benefitOrderId)
	*values = append(*values, seq, benefitOrderId)

	return sql.NullInt32{Valid: true, Int32: seq}
}

func getBenefitsSK(row []string, values *[]any) sql.NullInt32 {
	if len(row) <= benefitTypeIndex {
		return sql.NullInt32{Valid: false}
	}

	benefitId := row[benefitIdIndex]
	benefitType := row[benefitTypeIndex]

	if benefitId == "" {
		return sql.NullInt32{Valid: false}
	}

	seq, found := benefitsCache.Get(benefitId)
	if found {
		return sql.NullInt32{Valid: true, Int32: seq}
	}

	seq = benefitsCache.Set(benefitId)
	*values = append(*values, seq, benefitId, benefitType)

	return sql.NullInt32{Valid: true, Int32: seq}
}

func getBillingCurrenciesSK(row []string, values *[]any) sql.NullInt32 {
	if len(row) <= billingCurrencyIndex {
		return sql.NullInt32{Valid: false}
	}

	billingCurrency := row[billingCurrencyIndex]
	if billingCurrency == "" {
		return sql.NullInt32{Valid: false}
	}

	seq, found := billingCurrenciesCache.Get(billingCurrency)
	if found {
		return sql.NullInt32{Valid: true, Int32: seq}
	}

	seq = billingCurrenciesCache.Set(billingCurrency)
	*values = append(*values, seq, billingCurrency)

	return sql.NullInt32{Valid: true, Int32: seq}
}

func getChargeTypesSK(row []string, values *[]any) sql.NullInt32 {
	if len(row) <= chargeTypeIndex {
		return sql.NullInt32{Valid: false}
	}

	chargeType := row[chargeTypeIndex]
	if chargeType == "" {
		return sql.NullInt32{Valid: false}
	}

	seq, found := chargeTypesCache.Get(chargeType)
	if found {
		return sql.NullInt32{Valid: true, Int32: seq}
	}

	seq = chargeTypesCache.Set(chargeType)
	*values = append(*values, seq, chargeType)

	return sql.NullInt32{Valid: true, Int32: seq}
}

func getCustomersSK(row []string, values *[]any) sql.NullInt32 {
	if len(row) <= tier2MpnIdIndex {
		return sql.NullInt32{Valid: false}
	}

	customerId := row[customerIdIndex]
	if customerId == "" {
		return sql.NullInt32{Valid: false}
	}

	seq, found := customersCache.Get(customerId)
	if found {
		return sql.NullInt32{Valid: true, Int32: seq}
	}

	seq = customersCache.Set(customerId)

	customerName := row[customerNameIndex]
	customerDomain := row[customerDomainNameIndex]
	customerCountry := row[customerCountryIndex]
	tier2MpnId := row[tier2MpnIdIndex]

	*values = append(*values,
		seq,
		customerId,
		customerName,
		customerDomain,
		customerCountry,
		helpers.ToNullableInt64(tier2MpnId))

	return sql.NullInt32{Valid: true, Int32: seq}
}

func getEntitlementsSK(row []string, values *[]any) sql.NullInt32 {
	if len(row) <= entitlementDescriptionIndex {
		return sql.NullInt32{Valid: false}
	}

	entitlementId := row[entitlementIdIndex]
	description := row[entitlementDescriptionIndex]

	if entitlementId == "" {
		return sql.NullInt32{Valid: false}
	}

	seq, found := entitlementsCache.Get(entitlementId)
	if found {
		return sql.NullInt32{Valid: true, Int32: seq}
	}

	seq = entitlementsCache.Set(entitlementId)
	*values = append(*values, seq, entitlementId, description)

	return sql.NullInt32{Valid: true, Int32: seq}
}

func getMetersSK(row []string, values *[]any) sql.NullInt32 {
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

	seq, found := metersCache.Get(meterId)
	if found {
		return sql.NullInt32{Valid: true, Int32: seq}
	}

	seq = metersCache.Set(meterId)
	*values = append(*values,
		seq,
		meterId,
		name,
		category,
		meterType,
		subcategory,
		region,
		unit,
	)

	return sql.NullInt32{Valid: true, Int32: seq}
}

func getMonthsChargeDatesSK(row []string, values *[]any) sql.NullInt32 {
	if len(row) <= chargeEndDateIndex {
		return sql.NullInt32{Valid: false}
	}

	chargeStartDate := row[chargeStartDateIndex]
	chargeEndDate := row[chargeEndDateIndex]

	if chargeStartDate == "" || chargeEndDate == "" {
		return sql.NullInt32{Valid: false}
	}

	key := fmt.Sprintf("%s|%s", chargeStartDate, chargeEndDate)
	seq, found := monthsChargeDatesCache.Get(key)
	if found {
		return sql.NullInt32{Valid: true, Int32: seq}
	}

	seq = monthsChargeDatesCache.Set(key)
	*values = append(*values,
		seq,
		helpers.ToNullableDate(chargeStartDate),
		helpers.ToNullableDate(chargeEndDate),
	)

	return sql.NullInt32{Valid: true, Int32: seq}
}

func getPartnerCreditsSK(row []string, values *[]any) sql.NullInt32 {
	if len(row) <= partnerEarnedCreditPercentageIndex {
		return sql.NullInt32{Valid: false}
	}

	creditType := row[creditTypeIndex]
	percentage := row[creditPercentageIndex]
	partnerEarnedPercentage := row[partnerEarnedCreditPercentageIndex]

	if creditType == "" {
		return sql.NullInt32{Valid: false}
	}

	seq, found := partnerCreditsCache.Get(creditType)
	if found {
		return sql.NullInt32{Valid: true, Int32: seq}
	}

	seq = partnerCreditsCache.Set(creditType)
	*values = append(*values,
		seq,
		creditType,
		helpers.ToNullableFloat64(percentage),
		helpers.ToNullableFloat64(partnerEarnedPercentage),
	)

	return sql.NullInt32{Valid: true, Int32: seq}
}

func getPartnersSK(row []string, values *[]any) sql.NullInt32 {
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

	seq, found := partnersCache.Get(partnerId)
	if found {
		return sql.NullInt32{Valid: true, Int32: seq}
	}

	seq = partnersCache.Set(partnerId)
	*values = append(*values,
		seq,
		partnerId,
		partnerName,
		helpers.ToNullableInt64(mpnId),
		invoiceNumber,
	)
	return sql.NullInt32{Valid: true, Int32: seq}
}

func getPricingCurrenciesSK(row []string, values *[]any) sql.NullInt32 {
	if len(row) <= pricingCurrencyIndex {
		return sql.NullInt32{Valid: false}
	}

	pricingCurrency := row[pricingCurrencyIndex]
	if pricingCurrency == "" {
		return sql.NullInt32{Valid: false}
	}

	seq, found := pricingCurrenciesCache.Get(pricingCurrency)
	if found {
		return sql.NullInt32{Valid: true, Int32: seq}
	}

	seq = pricingCurrenciesCache.Set(pricingCurrency)
	*values = append(*values, seq, pricingCurrency)

	return sql.NullInt32{Valid: true, Int32: seq}
}

func getProductsSK(row []string, values *[]any) sql.NullInt32 {
	if len(row) <= productNameIndex {
		return sql.NullInt32{Valid: false}
	}

	productId := row[productIdIndex]
	productName := row[productNameIndex]

	if productId == "" {
		return sql.NullInt32{Valid: false}
	}

	seq, found := productsCache.Get(productId)
	if found {
		return sql.NullInt32{Valid: true, Int32: seq}
	}

	seq = productsCache.Set(productId)
	*values = append(*values, seq, productId, productName)

	return sql.NullInt32{Valid: true, Int32: seq}
}

func getPublishersSK(row []string, values *[]any) sql.NullInt32 {
	if len(row) <= publisherNameIndex {
		return sql.NullInt32{Valid: false}
	}

	publisherId := row[publisherIdIndex]
	publisherName := row[publisherNameIndex]

	if publisherId == "" {
		return sql.NullInt32{Valid: false}
	}

	seq, found := publishersCache.Get(publisherId)
	if found {
		return sql.NullInt32{Valid: true, Int32: seq}
	}

	seq = publishersCache.Set(publisherId)
	*values = append(*values, seq, publisherId, publisherName)

	return sql.NullInt32{Valid: true, Int32: seq}
}

func getResourceGroupsSK(row []string, values *[]any) sql.NullInt32 {
	if len(row) <= resourceGroupIndex {
		return sql.NullInt32{Valid: false}
	}

	name := row[resourceGroupIndex]
	if name == "" {
		return sql.NullInt32{Valid: false}
	}

	seq, found := resourceGroupsCache.Get(name)
	if found {
		return sql.NullInt32{Valid: true, Int32: seq}
	}

	seq = resourceGroupsCache.Set(name)
	*values = append(*values, seq, name)

	return sql.NullInt32{Valid: true, Int32: seq}
}

func getResourceLocationsSK(row []string, values *[]any) sql.NullInt32 {
	if len(row) <= resourceLocationIndex {
		return sql.NullInt32{Valid: false}
	}

	location := row[resourceLocationIndex]
	if location == "" {
		return sql.NullInt32{Valid: false}
	}

	seq, found := resourceLocationsCache.Get(location)
	if found {
		return sql.NullInt32{Valid: true, Int32: seq}
	}

	seq = resourceLocationsCache.Set(location)
	*values = append(*values, seq, location)

	return sql.NullInt32{Valid: true, Int32: seq}
}

func getServicesSK(row []string, values *[]any) sql.NullInt32 {
	if len(row) <= consumedServiceIndex {
		return sql.NullInt32{Valid: false}
	}

	service := row[consumedServiceIndex]
	if service == "" {
		return sql.NullInt32{Valid: false}
	}

	seq, found := servicesCache.Get(service)
	if found {
		return sql.NullInt32{Valid: true, Int32: seq}
	}

	seq = servicesCache.Set(service)
	*values = append(*values, seq, service)

	return sql.NullInt32{Valid: true, Int32: seq}
}

func getSkusSK(row []string, values *[]any) sql.NullInt32 {
	if len(row) <= skuNameIndex {
		return sql.NullInt32{Valid: false}
	}

	skuId := row[skuIdIndex]
	skuName := row[skuNameIndex]

	if skuId == "" {
		return sql.NullInt32{Valid: false}
	}

	seq, found := skusCache.Get(skuId)
	if found {
		return sql.NullInt32{Valid: true, Int32: seq}
	}

	seq = skusCache.Set(skuId)
	*values = append(*values, seq, skuId, skuName)

	return sql.NullInt32{Valid: true, Int32: seq}
}

func getSubscriptionsSK(row []string, values *[]any) sql.NullInt32 {
	subscriptionId := row[subscriptionIdIndex]
	description := row[subscriptionDescriptionIndex]

	if subscriptionId == "" {
		return sql.NullInt32{Valid: false}
	}

	seq, found := subscriptionsCache.Get(subscriptionId)
	if found {
		return sql.NullInt32{Valid: true, Int32: seq}
	}

	seq = subscriptionsCache.Set(subscriptionId)
	*values = append(*values, seq, subscriptionId, description)

	return sql.NullInt32{Valid: true, Int32: seq}
}

func getUnitTypesSK(row []string, values *[]any) sql.NullInt32 {
	if len(row) <= unitTypeIndex {
		return sql.NullInt32{Valid: false}
	}

	unitType := row[unitTypeIndex]
	if unitType == "" {
		return sql.NullInt32{Valid: false}
	}

	seq, found := unitTypesCache.Get(unitType)
	if found {
		return sql.NullInt32{Valid: true, Int32: seq}
	}

	seq = unitTypesCache.Set(unitType)
	*values = append(*values, seq, unitType)

	return sql.NullInt32{Valid: true, Int32: seq}
}

func getUsageDatesSK(row []string, values *[]any) sql.NullInt32 {
	if len(row) <= usageDateIndex {
		return sql.NullInt32{Valid: false}
	}

	usageDate := row[usageDateIndex]
	if usageDate == "" {
		return sql.NullInt32{Valid: false}
	}

	seq, found := usageDatesCache.Get(usageDate)
	if found {
		return sql.NullInt32{Valid: true, Int32: seq}
	}

	seq = usageDatesCache.Set(usageDate)
	*values = append(*values, seq, helpers.ToNullableDate(usageDate))

	return sql.NullInt32{Valid: true, Int32: seq}
}
