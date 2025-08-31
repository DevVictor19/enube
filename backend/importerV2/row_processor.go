package importerV2

import (
	"encoding/csv"
	"fmt"
	"log"
	"os"
	"sync"

	"github.com/DevVictor19/enube/backend/importerV2/database"
	"github.com/DevVictor19/enube/backend/importerV2/helpers"
)

type insertValues struct {
	availabilities    []any
	benefitOrders     []any
	benefits          []any
	billingCurrencies []any
	chargeTypes       []any
	customers         []any
	entitlements      []any
	meters            []any
	monthsChargeDates []any
	partnerCredits    []any
	partners          []any
	pricingCurrencies []any
	products          []any
	publishers        []any
	resourceGroups    []any
	resourceLocations []any
	services          []any
	skus              []any
	subscriptions     []any
	unitTypes         []any
	usageDates        []any
	factCharges       []any
}

func processCSV(filepath string, maxRowsToProcess int) {
	file, err := os.Open(filepath)
	if err != nil {
		log.Fatal(err)
	}
	defer file.Close()

	var wg sync.WaitGroup
	values := insertValues{}
	rowsProcessed := 0
	reader := csv.NewReader(file)

	for {
		row, err := reader.Read()
		if err != nil {
			if err.Error() == "EOF" {
				break
			}
			log.Fatal(err)
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

		values.factCharges = append(values.factCharges,
			getPartnersSK(row, &values.partners),
			getMonthsChargeDatesSK(row, &values.monthsChargeDates),
			getCustomersSK(row, &values.customers),
			getMetersSK(row, &values.meters),
			getProductsSK(row, &values.products),
			getSkusSK(row, &values.skus),
			getPublishersSK(row, &values.publishers),
			getSubscriptionsSK(row, &values.subscriptions),
			getResourceLocationsSK(row, &values.resourceLocations),
			getResourceGroupsSK(row, &values.resourceGroups),
			getServicesSK(row, &values.services),
			getChargeTypesSK(row, &values.chargeTypes),
			getUnitTypesSK(row, &values.unitTypes),
			getEntitlementsSK(row, &values.entitlements),
			getPartnerCreditsSK(row, &values.partnerCredits),
			getBenefitsSK(row, &values.benefits),
			getBenefitOrdersSK(row, &values.benefitOrders),
			getAvailabilitySK(row, &values.availabilities),
			getUsageDatesSK(row, &values.usageDates),
			getBillingCurrenciesSK(row, &values.billingCurrencies),
			getPricingCurrenciesSK(row, &values.pricingCurrencies),
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
			additionalInfo)

		rowsProcessed++

		if rowsProcessed == maxRowsToProcess {
			fmt.Printf("Processing batch of %d rows\n", rowsProcessed)
			wg.Add(1)
			go func(v insertValues) {
				defer wg.Done()
				bachInsert(v)
			}(values)
			values = insertValues{}
			rowsProcessed = 0
		}
	}

	wg.Wait()

	if rowsProcessed > 0 {
		fmt.Printf("Processing batch of %d rows\n", rowsProcessed)
		bachInsert(values)
	}
}

func bachInsert(values insertValues) {
	db, err := database.Get()
	if err != nil {
		log.Fatal(err)
	}

	tx, err := db.Begin()
	if err != nil {
		log.Fatal(err)
	}

	if _, err := tx.Exec(getAvailabilityStm(values.availabilities), values.availabilities...); err != nil {
		tx.Rollback()
		fmt.Println(availabilitiesCache.vMap)
		fmt.Println(values.availabilities)
		log.Fatal("dim_availabilities err:", err)
	}

	if _, err := tx.Exec(getBenefitOrderStm(values.benefitOrders), values.benefitOrders...); err != nil {
		tx.Rollback()
		fmt.Println(benefitOrdersCache.vMap)
		fmt.Println(values.benefitOrders)
		log.Fatal("dim_benefit_order err:", err)
	}

	if _, err := tx.Exec(getBenefitStm(values.benefits), values.benefits...); err != nil {
		tx.Rollback()
		fmt.Println(benefitsCache.vMap)
		fmt.Println(values.benefits)
		log.Fatal("dim_benefit err:", err)
	}

	if _, err := tx.Exec(getChargeTypeStm(values.chargeTypes), values.chargeTypes...); err != nil {
		tx.Rollback()
		fmt.Println(chargeTypesCache.vMap)
		fmt.Println(values.chargeTypes)
		log.Fatal("dim_charge_type err:", err)
	}

	if _, err := tx.Exec(getCustomerStm(values.customers), values.customers...); err != nil {
		tx.Rollback()
		fmt.Println(customersCache.vMap)
		fmt.Println(values.customers)
		log.Fatal("dim_customers err:", err)
	}

	if _, err := tx.Exec(getEntitlementStm(values.entitlements), values.entitlements...); err != nil {
		tx.Rollback()
		fmt.Println(entitlementsCache.vMap)
		fmt.Println(values.entitlements)
		log.Fatal("dim_entitlement err:", err)
	}

	if _, err := tx.Exec(getMeterStm(values.meters), values.meters...); err != nil {
		tx.Rollback()
		fmt.Println(metersCache.vMap)
		fmt.Println(values.meters)
		log.Fatal("dim_meters err:", err)
	}

	if _, err := tx.Exec(getMonthsChargeDateStm(values.monthsChargeDates), values.monthsChargeDates...); err != nil {
		tx.Rollback()
		fmt.Println(monthsChargeDatesCache.vMap)
		fmt.Println(values.monthsChargeDates)
		log.Fatal("dim_months_charge_date err:", err)
	}

	if _, err := tx.Exec(getPartnerCreditStm(values.partnerCredits), values.partnerCredits...); err != nil {
		tx.Rollback()
		fmt.Println(partnerCreditsCache.vMap)
		fmt.Println(values.partnerCredits)
		log.Fatal("dim_partner_credit err:", err)
	}

	if _, err := tx.Exec(getPartnerStm(values.partners), values.partners...); err != nil {
		tx.Rollback()
		fmt.Println(partnersCache.vMap)
		fmt.Println(values.partners)
		log.Fatal("dim_partner err:", err)
	}

	if _, err := tx.Exec(getProductStm(values.products), values.products...); err != nil {
		tx.Rollback()
		fmt.Println(productsCache.vMap)
		fmt.Println(values.products)
		log.Fatal("dim_product err:", err)
	}

	if _, err := tx.Exec(getPublisherStm(values.publishers), values.publishers...); err != nil {
		tx.Rollback()
		fmt.Println(publishersCache.vMap)
		fmt.Println(values.publishers)
		log.Fatal("dim_publisher err:", err)
	}

	if _, err := tx.Exec(getResourceGroupStm(values.resourceGroups), values.resourceGroups...); err != nil {
		tx.Rollback()
		fmt.Println(resourceGroupsCache.vMap)
		fmt.Println(values.resourceGroups)
		log.Fatal("dim_resource_group err:", err)
	}

	if _, err := tx.Exec(getResourceLocationStm(values.resourceLocations), values.resourceLocations...); err != nil {
		tx.Rollback()
		fmt.Println(resourceLocationsCache.vMap)
		fmt.Println(values.resourceLocations)
		log.Fatal("dim_resource_location err:", err)
	}

	if _, err := tx.Exec(getServiceStm(values.services), values.services...); err != nil {
		tx.Rollback()
		fmt.Println(servicesCache.vMap)
		fmt.Println(values.services)
		log.Fatal("dim_service err:", err)
	}

	if _, err := tx.Exec(getSkuStm(values.skus), values.skus...); err != nil {
		tx.Rollback()
		fmt.Println(skusCache.vMap)
		fmt.Println(values.skus)
		log.Fatal("dim_sku err:", err)
	}

	if _, err := tx.Exec(getSubscriptionStm(values.subscriptions), values.subscriptions...); err != nil {
		tx.Rollback()
		fmt.Println(subscriptionsCache.vMap)
		fmt.Println(values.subscriptions)
		log.Fatal("dim_subscription err:", err)
	}

	if _, err := tx.Exec(getUnitTypeStm(values.unitTypes), values.unitTypes...); err != nil {
		tx.Rollback()
		fmt.Println(unitTypesCache.vMap)
		fmt.Println(values.unitTypes)
		log.Fatal("dim_unit_type err:", err)
	}

	if _, err := tx.Exec(getUsageDateStm(values.usageDates), values.usageDates...); err != nil {
		tx.Rollback()
		fmt.Println(usageDatesCache.vMap)
		fmt.Println(values.usageDates)
		log.Fatal("dim_usage_dates err:", err)
	}

	if _, err := tx.Exec(getBillingCurrencyStm(values.billingCurrencies), values.billingCurrencies...); err != nil {
		tx.Rollback()
		fmt.Println(billingCurrenciesCache.vMap)
		fmt.Println(values.billingCurrencies)
		log.Fatal("dim_billing_currencies err:", err)
	}

	if _, err := tx.Exec(getPricingCurrencyStm(values.pricingCurrencies), values.pricingCurrencies...); err != nil {
		tx.Rollback()
		fmt.Println(pricingCurrenciesCache.vMap)
		fmt.Println(values.pricingCurrencies)
		log.Fatal("dim_pricing_currencies err:", err)
	}

	if _, err := tx.Exec(getFactChargesStm(values.factCharges), values.factCharges...); err != nil {
		tx.Rollback()
		log.Fatal("fact_charge err:", err)
	}

	if err := tx.Commit(); err != nil {
		log.Fatal("Err on commit:", err)
	}
}
