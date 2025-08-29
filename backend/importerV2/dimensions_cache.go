package importerV2

import (
	"database/sql"
	"fmt"

	"github.com/DevVictor19/enube/backend/importerV2/helpers"
)

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

	seq, new := benefitOrdersCache.NewEntry(benefitOrderId)
	if new {
		values := []any{
			seq,
			benefitOrderId,
		}

		return sql.NullInt32{Valid: true, Int32: seq}, values
	}

	return sql.NullInt32{Valid: true, Int32: seq}, nil
}

func getBenefits(row []string) (sql.NullInt32, []any) {
	if len(row) <= benefitTypeIndex {
		return sql.NullInt32{Valid: false}, nil
	}

	benefitId := row[benefitIdIndex]
	benefitType := row[benefitTypeIndex]

	if benefitId == "" {
		return sql.NullInt32{Valid: false}, nil
	}

	seq, new := benefitsCache.NewEntry(benefitId)
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

func getBillingCurrencies(row []string) (sql.NullInt32, []any) {
	billingCurrency := row[billingCurrencyIndex]

	if billingCurrency == "" {
		return sql.NullInt32{Valid: false}, nil
	}

	seq, new := billingCurrenciesCache.NewEntry(billingCurrency)
	if new {
		values := []any{
			seq,
			billingCurrency,
		}

		return sql.NullInt32{Valid: true, Int32: seq}, values
	}

	return sql.NullInt32{Valid: true, Int32: seq}, nil
}

func getChargeTypes(row []string) (sql.NullInt32, []any) {
	if len(row) <= chargeTypeIndex {
		return sql.NullInt32{Valid: false}, nil
	}

	chargeType := row[chargeTypeIndex]
	if chargeType == "" {
		return sql.NullInt32{Valid: false}, nil
	}

	seq, new := chargeTypesCache.NewEntry(chargeType)
	if new {
		values := []any{
			seq,
			chargeType,
		}

		return sql.NullInt32{Valid: true, Int32: seq}, values
	}

	return sql.NullInt32{Valid: true, Int32: seq}, nil
}

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

func getEntitlements(row []string) (sql.NullInt32, []any) {
	if len(row) <= entitlementDescriptionIndex {
		return sql.NullInt32{Valid: false}, nil
	}

	entitlementId := row[entitlementIdIndex]
	description := row[entitlementDescriptionIndex]

	if entitlementId == "" {
		return sql.NullInt32{Valid: false}, nil
	}

	seq, new := entitlementsCache.NewEntry(entitlementId)
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

	seq, new := metersCache.NewEntry(meterId)
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
	seq, new := monthsChargeDatesCache.NewEntry(key)
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

	seq, new := partnerCreditsCache.NewEntry(creditType)
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

	seq, new := partnersCache.NewEntry(partnerId)
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

func getPricingCurrencies(row []string) (sql.NullInt32, []any) {
	pricingCurrency := row[pricingCurrencyIndex]

	if pricingCurrency == "" {
		return sql.NullInt32{Valid: false}, nil
	}

	seq, new := pricingCurrenciesCache.NewEntry(pricingCurrency)
	if new {
		values := []any{
			seq,
			pricingCurrency,
		}
		return sql.NullInt32{Valid: true, Int32: seq}, values
	}

	return sql.NullInt32{Valid: true, Int32: seq}, nil
}

func getProducts(row []string) (sql.NullInt32, []any) {
	if len(row) <= productNameIndex {
		return sql.NullInt32{Valid: false}, nil
	}

	productId := row[productIdIndex]
	productName := row[productNameIndex]

	if productId == "" {
		return sql.NullInt32{Valid: false}, nil
	}

	seq, new := productsCache.NewEntry(productId)
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

func getPublishers(row []string) (sql.NullInt32, []any) {
	if len(row) <= publisherNameIndex {
		return sql.NullInt32{Valid: false}, nil
	}

	publisherId := row[publisherIdIndex]
	publisherName := row[publisherNameIndex]

	if publisherId == "" {
		return sql.NullInt32{Valid: false}, nil
	}

	seq, new := publishersCache.NewEntry(publisherId)
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

func getResourceGroups(row []string) (sql.NullInt32, []any) {
	if len(row) <= resourceGroupIndex {
		return sql.NullInt32{Valid: false}, nil
	}

	name := row[resourceGroupIndex]
	if name == "" {
		return sql.NullInt32{Valid: false}, nil
	}

	seq, new := resourceGroupsCache.NewEntry(name)
	if new {
		values := []any{
			seq,
			name,
		}
		return sql.NullInt32{Valid: true, Int32: seq}, values
	}

	return sql.NullInt32{Valid: true, Int32: seq}, nil
}

func getResourceLocations(row []string) (sql.NullInt32, []any) {
	if len(row) <= resourceLocationIndex {
		return sql.NullInt32{Valid: false}, nil
	}

	location := row[resourceLocationIndex]
	if location == "" {
		return sql.NullInt32{Valid: false}, nil
	}

	seq, new := resourceLocationsCache.NewEntry(location)
	if new {
		values := []any{
			seq,
			location,
		}
		return sql.NullInt32{Valid: true, Int32: seq}, values
	}

	return sql.NullInt32{Valid: true, Int32: seq}, nil
}

func getServices(row []string) (sql.NullInt32, []any) {
	if len(row) <= consumedServiceIndex {
		return sql.NullInt32{Valid: false}, nil
	}

	service := row[consumedServiceIndex]
	if service == "" {
		return sql.NullInt32{Valid: false}, nil
	}

	seq, new := servicesCache.NewEntry(service)
	if new {
		values := []any{
			seq,
			service,
		}
		return sql.NullInt32{Valid: true, Int32: seq}, values
	}

	return sql.NullInt32{Valid: true, Int32: seq}, nil
}

func getSkus(row []string) (sql.NullInt32, []any) {
	if len(row) <= skuNameIndex {
		return sql.NullInt32{Valid: false}, nil
	}

	skuId := row[skuIdIndex]
	skuName := row[skuNameIndex]

	if skuId == "" {
		return sql.NullInt32{Valid: false}, nil
	}

	seq, new := skusCache.NewEntry(skuId)
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

func getSubscriptions(row []string) (sql.NullInt32, []any) {
	subscriptionId := row[subscriptionIdIndex]
	description := row[subscriptionDescriptionIndex]

	if subscriptionId == "" {
		return sql.NullInt32{Valid: false}, nil
	}

	seq, new := subscriptionsCache.NewEntry(subscriptionId)
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

func getUnitTypes(row []string) (sql.NullInt32, []any) {
	unitType := row[unitTypeIndex]

	if unitType == "" {
		return sql.NullInt32{Valid: false}, nil
	}

	seq, new := unitTypesCache.NewEntry(unitType)
	if new {
		values := []any{
			seq,
			unitType,
		}
		return sql.NullInt32{Valid: true, Int32: seq}, values
	}

	return sql.NullInt32{Valid: true, Int32: seq}, nil
}

func getUsageDates(row []string) (sql.NullInt32, []any) {
	usageDate := row[usageDateIndex]

	if usageDate == "" {
		return sql.NullInt32{Valid: false}, nil
	}

	seq, new := usageDatesCache.NewEntry(usageDate)
	if new {
		values := []any{
			seq,
			helpers.ToNullableDate(usageDate),
		}
		return sql.NullInt32{Valid: true, Int32: seq}, values
	}

	return sql.NullInt32{Valid: true, Int32: seq}, nil
}
