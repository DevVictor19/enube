package importerV2

import (
	"fmt"
)

func getAvailabilitySK(row []string) (string, []string) {
	if len(row) <= availabilityIdIndex {
		return "", nil
	}

	availabilityId := row[availabilityIdIndex]
	if availabilityId == "" {
		return "", nil
	}

	seq, new := availabilitiesCache.NewEntry(availabilityId)
	if new {
		return seq, []string{seq, availabilityId}
	}

	return seq, nil
}

func getBenefitOrdersSK(row []string) (string, []string) {
	if len(row) <= benefitOrderIdIndex {
		return "", nil
	}

	benefitOrderId := row[benefitOrderIdIndex]
	if benefitOrderId == "" {
		return "", nil
	}

	seq, new := benefitOrdersCache.NewEntry(benefitOrderId)
	if new {
		return seq, []string{seq, benefitOrderId}
	}

	return seq, nil
}

func getBenefitsSK(row []string) (string, []string) {
	if len(row) <= benefitTypeIndex {
		return "", nil
	}

	benefitId := row[benefitIdIndex]
	benefitType := row[benefitTypeIndex]

	if benefitId == "" {
		return "", nil
	}

	seq, new := benefitsCache.NewEntry(benefitId)
	if new {
		return seq, []string{seq, benefitId, benefitType}
	}

	return seq, nil
}

func getBillingCurrenciesSK(row []string) (string, []string) {
	if len(row) <= billingCurrencyIndex {
		return "", nil
	}

	billingCurrency := row[billingCurrencyIndex]
	if billingCurrency == "" {
		return "", nil
	}

	seq, new := billingCurrenciesCache.NewEntry(billingCurrency)
	if new {
		return seq, []string{seq, billingCurrency}
	}

	return seq, nil
}

func getChargeTypesSK(row []string) (string, []string) {
	if len(row) <= chargeTypeIndex {
		return "", nil
	}

	chargeType := row[chargeTypeIndex]
	if chargeType == "" {
		return "", nil
	}

	seq, new := chargeTypesCache.NewEntry(chargeType)
	if new {
		return seq, []string{seq, chargeType}
	}

	return seq, nil
}

func getCustomersSK(row []string) (string, []string) {
	if len(row) <= tier2MpnIdIndex {
		return "", nil
	}

	customerId := row[customerIdIndex]
	if customerId == "" {
		return "", nil
	}

	seq, new := customersCache.NewEntry(customerId)
	if new {
		customerName := row[customerNameIndex]
		customerDomain := row[customerDomainNameIndex]
		customerCountry := row[customerCountryIndex]
		tier2MpnId := row[tier2MpnIdIndex]

		return seq, []string{seq, customerId,
			customerName,
			customerDomain,
			customerCountry,
			tier2MpnId,
		}
	}

	return seq, nil
}

func getEntitlementsSK(row []string) (string, []string) {
	if len(row) <= entitlementDescriptionIndex {
		return "", nil
	}

	entitlementId := row[entitlementIdIndex]
	description := row[entitlementDescriptionIndex]

	if entitlementId == "" {
		return "", nil
	}

	seq, new := entitlementsCache.NewEntry(entitlementId)
	if new {
		return seq, []string{seq, entitlementId, description}
	}

	return seq, nil
}

func getMetersSK(row []string) (string, []string) {
	if len(row) <= meterUnitIndex {
		return "", nil
	}

	meterId := row[meterIdIndex]

	if meterId == "" {
		return "", nil
	}

	seq, new := metersCache.NewEntry(meterId)
	if new {
		name := row[meterNameIndex]
		category := row[meterCategoryIndex]
		meterType := row[meterTypeIndex]
		subcategory := row[meterSubCategoryIndex]
		region := row[meterRegionIndex]
		unit := row[meterUnitIndex]
		return seq, []string{seq,
			meterId,
			name,
			category,
			meterType,
			subcategory,
			region,
			unit,
		}
	}

	return seq, nil
}

func getMonthsChargeDatesSK(row []string) (string, []string) {
	if len(row) <= chargeEndDateIndex {
		return "", nil
	}

	chargeStartDate := row[chargeStartDateIndex]
	chargeEndDate := row[chargeEndDateIndex]

	if chargeStartDate == "" || chargeEndDate == "" {
		return "", nil
	}

	key := fmt.Sprintf("%s|%s", chargeStartDate, chargeEndDate)
	seq, new := monthsChargeDatesCache.NewEntry(key)
	if new {
		return seq, []string{seq,
			chargeStartDate,
			chargeEndDate,
		}
	}

	return seq, nil
}

func getPartnerCreditsSK(row []string) (string, []string) {
	if len(row) <= partnerEarnedCreditPercentageIndex {
		return "", nil
	}

	creditType := row[creditTypeIndex]
	percentage := row[creditPercentageIndex]
	partnerEarnedPercentage := row[partnerEarnedCreditPercentageIndex]

	if creditType == "" {
		return "", nil
	}

	seq, new := partnerCreditsCache.NewEntry(creditType)
	if new {
		return seq, []string{seq,
			creditType,
			percentage,
			partnerEarnedPercentage,
		}
	}

	return seq, nil
}

func getPartnersSK(row []string) (string, []string) {
	if len(row) <= invoiceNumberIndex {
		return "", nil
	}

	partnerId := row[partnerIdIndex]

	if partnerId == "" {
		return "", nil
	}

	seq, new := partnersCache.NewEntry(partnerId)
	if new {
		partnerName := row[partnerNameIndex]
		mpnId := row[mpnIdIndex]
		invoiceNumber := row[invoiceNumberIndex]
		return seq, []string{seq,
			partnerId,
			partnerName,
			mpnId,
			invoiceNumber,
		}
	}

	return seq, nil
}

func getPricingCurrenciesSK(row []string) (string, []string) {
	if len(row) <= pricingCurrencyIndex {
		return "", nil
	}

	pricingCurrency := row[pricingCurrencyIndex]
	if pricingCurrency == "" {
		return "", nil
	}

	seq, new := pricingCurrenciesCache.NewEntry(pricingCurrency)
	if new {
		return seq, []string{seq, pricingCurrency}
	}

	return seq, nil
}

func getProductsSK(row []string) (string, []string) {
	if len(row) <= productNameIndex {
		return "", nil
	}

	productId := row[productIdIndex]
	productName := row[productNameIndex]

	if productId == "" {
		return "", nil
	}

	seq, new := productsCache.NewEntry(productId)
	if new {
		return seq, []string{seq, productId, productName}
	}

	return seq, nil
}

func getPublishersSK(row []string) (string, []string) {
	if len(row) <= publisherNameIndex {
		return "", nil
	}

	publisherId := row[publisherIdIndex]
	publisherName := row[publisherNameIndex]

	if publisherId == "" {
		return "", nil
	}

	seq, new := publishersCache.NewEntry(publisherId)
	if new {
		return seq, []string{seq, publisherId, publisherName}
	}

	return seq, nil
}

func getResourceGroupsSK(row []string) (string, []string) {
	if len(row) <= resourceGroupIndex {
		return "", nil
	}

	name := row[resourceGroupIndex]
	if name == "" {
		return "", nil
	}

	seq, new := resourceGroupsCache.NewEntry(name)
	if new {
		return seq, []string{seq, name}
	}

	return seq, nil
}

func getResourceLocationsSK(row []string) (string, []string) {
	if len(row) <= resourceLocationIndex {
		return "", nil
	}

	location := row[resourceLocationIndex]
	if location == "" {
		return "", nil
	}

	seq, new := resourceLocationsCache.NewEntry(location)
	if new {
		return seq, []string{seq, location}
	}

	return seq, nil
}

func getServicesSK(row []string) (string, []string) {
	if len(row) <= consumedServiceIndex {
		return "", nil
	}

	service := row[consumedServiceIndex]
	if service == "" {
		return "", nil
	}

	seq, new := servicesCache.NewEntry(service)
	if new {
		return seq, []string{seq, service}
	}

	return seq, nil
}

func getSkusSK(row []string) (string, []string) {
	if len(row) <= skuNameIndex {
		return "", nil
	}

	skuId := row[skuIdIndex]
	skuName := row[skuNameIndex]

	if skuId == "" {
		return "", nil
	}

	seq, new := skusCache.NewEntry(skuId)
	if new {
		return seq, []string{seq, skuId, skuName}
	}

	return seq, nil
}

func getSubscriptionsSK(row []string) (string, []string) {
	subscriptionId := row[subscriptionIdIndex]
	description := row[subscriptionDescriptionIndex]

	if subscriptionId == "" {
		return "", nil
	}

	seq, new := subscriptionsCache.NewEntry(subscriptionId)
	if new {
		return seq, []string{seq, subscriptionId, description}
	}

	return seq, nil
}

func getUnitTypesSK(row []string) (string, []string) {
	if len(row) <= unitTypeIndex {
		return "", nil
	}

	unitType := row[unitTypeIndex]
	if unitType == "" {
		return "", nil
	}

	seq, new := unitTypesCache.NewEntry(unitType)
	if new {
		return seq, []string{seq, unitType}
	}

	return seq, nil
}

func getUsageDatesSK(row []string) (string, []string) {
	if len(row) <= usageDateIndex {
		return "", nil
	}

	usageDate := row[usageDateIndex]
	if usageDate == "" {
		return "", nil
	}

	seq, new := usageDatesCache.NewEntry(usageDate)
	if new {
		return seq, []string{seq, usageDate}
	}

	return seq, nil
}
