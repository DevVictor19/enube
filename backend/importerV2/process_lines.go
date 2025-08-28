package importerV2

import (
	"fmt"
	"sync"

	"github.com/DevVictor19/enube/backend/importerV2/helpers"
)

var mu sync.Mutex
var c = make(chan map[tableName][]any)

func prepareStatement(row []string) {
	defer mu.Unlock()
	mu.Lock()

	vMap := make(map[tableName][]any)

	customerSK, values := getCustomers(row)
	if values != nil {
		vMap[customersTable] = values
	}
	chargeTypeSK, values := getChargeTypes(row)
	if values != nil {
		vMap[chargeTypesTable] = values
	}
	billingCurrencySK, values := getBillingCurrencies(row)
	if values != nil {
		vMap[billingCurrenciesTable] = values
	}
	entitlementSK, values := getEntitlements(row)
	if values != nil {
		vMap[entitlementsTable] = values
	}
	meterSK, values := getMeters(row)
	if values != nil {
		vMap[metersTable] = values
	}
	monthsChargeDateSK, values := getMonthsChargeDates(row)
	if values != nil {
		vMap[monthsChargeDatesTable] = values
	}
	partnerCreditSK, values := getPartnerCredits(row)
	if values != nil {
		vMap[partnerCreditsTable] = values
	}
	partnerSK, values := getPartners(row)
	if values != nil {
		vMap[partnersTable] = values
	}
	pricingCurrencySK, values := getPricingCurrencies(row)
	if values != nil {
		vMap[pricingCurrenciesTable] = values
	}
	productSK, values := getProducts(row)
	if values != nil {
		vMap[productsTable] = values
	}
	publisherSK, values := getPublishers(row)
	if values != nil {
		vMap[publishersTable] = values
	}
	resourceGroupSK, values := getResourceGroups(row)
	if values != nil {
		vMap[resourceGroupsTable] = values
	}
	resourceLocationSK, values := getResourceLocations(row)
	if values != nil {
		vMap[resourceLocationsTable] = values
	}
	serviceSK, values := getServices(row)
	if values != nil {
		vMap[servicesTable] = values
	}
	skuSK, values := getSkus(row)
	if values != nil {
		vMap[skusTable] = values
	}
	subscriptionSK, values := getSubscriptions(row)
	if values != nil {
		vMap[subscriptionsTable] = values
	}
	unitTypeSK, values := getUnitTypes(row)
	if values != nil {
		vMap[unitTypesTable] = values
	}
	usageDateSK, values := getUsageDates(row)
	if values != nil {
		vMap[usageDatesTable] = values
	}
	benefitsSK, values := getBenefits(row)
	if values != nil {
		vMap[benefitsTable] = values
	}
	benefitsOrderSK, values := getBenefitOrders(row)
	if values != nil {
		vMap[benefitOrdersTable] = values
	}
	availabilitySK, values := getAvailability(row)
	if values != nil {
		vMap[availabilitiesTable] = values
	}

	resourceUri := row[resourceUriIndex]
	effectiveUnitPrice := row[effectiveUnitPriceIndex]
	unitPrice := row[unitPriceIndex]
	quantity := row[quantityIndex]
	billingPreTaxTotal := row[billingPreTaxTotalIndex]
	pricingPreTaxTotal := row[pricingPreTaxTotalIndex]
	pcToBcExchangeRate := row[pcToBcExchangeRateIndex]
	pcToBcExchangeRateDate := row[pcToBcExchangeRateDateIndex]
	serviceInfo1 := row[serviceInfo1Index]
	serviceInfo2 := row[serviceInfo2Index]
	tags := row[tagsIndex]
	additionalInfo := row[additionalInfoIndex]

	values = []any{
		partnerSK,
		monthsChargeDateSK,
		customerSK,
		meterSK,
		productSK,
		skuSK,
		publisherSK,
		subscriptionSK,
		resourceLocationSK,
		resourceGroupSK,
		serviceSK,
		chargeTypeSK,
		unitTypeSK,
		entitlementSK,
		partnerCreditSK,
		benefitsSK,
		benefitsOrderSK,
		availabilitySK,
		usageDateSK,
		billingCurrencySK,
		pricingCurrencySK,
		resourceUri,
		helpers.ToNullableFloat64(effectiveUnitPrice),
		helpers.ToNullableFloat64(unitPrice),
		helpers.ToNullableFloat64(quantity),
		helpers.ToNullableFloat64(billingPreTaxTotal),
		helpers.ToNullableFloat64(pricingPreTaxTotal),
		helpers.ToNullableFloat64(pcToBcExchangeRate),
		helpers.ToNullableDate(pcToBcExchangeRateDate),
		serviceInfo1,
		serviceInfo2,
		tags,
		additionalInfo,
	}

	vMap[factChargesTable] = values

	c <- vMap
}

func batchMonitor(maxLinesPerBatch int) {
	fmt.Println("Initializing batch monitor... maxLinesPerBatch:", maxLinesPerBatch)
	lines := 0

	for {
		vMap := <-c
		if len(vMap) > 0 {
			fmt.Println(vMap)
		}
		lines++

		if lines >= maxLinesPerBatch {
			fmt.Println("Executing batch....")
			lines = 0
		}
	}
}
