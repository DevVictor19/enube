package importerV2

import (
	"fmt"
	"log"
	"sync"

	"github.com/DevVictor19/enube/backend/importerV2/database"
	"github.com/DevVictor19/enube/backend/importerV2/helpers"
)

var (
	insertsWG        sync.WaitGroup
	valuesMap        = insert{}
	rowsProcessed    = 0
	maxRowsToProcess = 1500
)

type insert struct {
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

func processRow(row []string) {
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

	valuesMap.factCharges = append(valuesMap.factCharges,
		getPartnersSK(row),
		getMonthsChargeDatesSK(row),
		getCustomersSK(row),
		getMetersSK(row),
		getProductsSK(row),
		getSkusSK(row),
		getPublishersSK(row),
		getSubscriptionsSK(row),
		getResourceLocationsSK(row),
		getResourceGroupsSK(row),
		getServicesSK(row),
		getChargeTypesSK(row),
		getUnitTypesSK(row),
		getEntitlementsSK(row),
		getPartnerCreditsSK(row),
		getBenefitsSK(row),
		getBenefitOrdersSK(row),
		getAvailabilitySK(row),
		getUsageDatesSK(row),
		getBillingCurrenciesSK(row),
		getPricingCurrenciesSK(row),
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
		insertsWG.Add(1)
		go bachInsert(valuesMap, rowsProcessed, &insertsWG)
		valuesMap = insert{}
		rowsProcessed = 0
	}
}

func bachInsert(valuesMap insert, rows int, wg *sync.WaitGroup) {
	if wg != nil {
		defer wg.Done()
	}

	fmt.Printf("Executing batch insert of %d rows....\n", rows)

	db, err := database.Get()
	if err != nil {
		log.Fatal(err)
	}

	tx, err := db.Begin()
	if err != nil {
		log.Fatal(err)
	}

	if _, err := tx.Exec(getAvailabilityStm(valuesMap.availabilities), valuesMap.availabilities...); err != nil {
		tx.Rollback()
		log.Fatal("dim_availabilities err:", err)
	}

	if _, err := tx.Exec(getBenefitOrderStm(valuesMap.benefitOrders), valuesMap.benefitOrders...); err != nil {
		tx.Rollback()
		log.Fatal("dim_benefit_order err:", err)
	}

	if _, err := tx.Exec(getBenefitStm(valuesMap.benefits), valuesMap.benefits...); err != nil {
		tx.Rollback()
		log.Fatal("dim_benefit err:", err)
	}

	if _, err := tx.Exec(getChargeTypeStm(valuesMap.chargeTypes), valuesMap.chargeTypes...); err != nil {
		tx.Rollback()
		log.Fatal("dim_charge_type err:", err)
	}

	if _, err := tx.Exec(getCustomerStm(valuesMap.customers), valuesMap.customers...); err != nil {
		tx.Rollback()
		log.Fatal("dim_customers err:", err)
	}

	if _, err := tx.Exec(getEntitlementStm(valuesMap.entitlements), valuesMap.entitlements...); err != nil {
		tx.Rollback()
		log.Fatal("dim_entitlement err:", err)
	}

	if _, err := tx.Exec(getMeterStm(valuesMap.meters), valuesMap.meters...); err != nil {
		tx.Rollback()
		log.Fatal("dim_meters err:", err)
	}

	if _, err := tx.Exec(getMonthsChargeDateStm(valuesMap.monthsChargeDates), valuesMap.monthsChargeDates...); err != nil {
		tx.Rollback()
		log.Fatal("dim_months_charge_date err:", err)
	}

	if _, err := tx.Exec(getPartnerCreditStm(valuesMap.partnerCredits), valuesMap.partnerCredits...); err != nil {
		tx.Rollback()
		log.Fatal("dim_partner_credit err:", err)
	}

	if _, err := tx.Exec(getPartnerStm(valuesMap.partners), valuesMap.partners...); err != nil {
		tx.Rollback()
		log.Fatal("dim_partner err:", err)
	}

	if _, err := tx.Exec(getProductStm(valuesMap.products), valuesMap.products...); err != nil {
		tx.Rollback()
		log.Fatal("dim_product err:", err)
	}

	if _, err := tx.Exec(getPublisherStm(valuesMap.publishers), valuesMap.publishers...); err != nil {
		tx.Rollback()
		log.Fatal("dim_publisher err:", err)
	}

	if _, err := tx.Exec(getResourceGroupStm(valuesMap.resourceGroups), valuesMap.resourceGroups...); err != nil {
		tx.Rollback()
		log.Fatal("dim_resource_group err:", err)
	}

	if _, err := tx.Exec(getResourceLocationStm(valuesMap.resourceLocations), valuesMap.resourceLocations...); err != nil {
		tx.Rollback()
		log.Fatal("dim_resource_location err:", err)
	}

	if _, err := tx.Exec(getServiceStm(valuesMap.services), valuesMap.services...); err != nil {
		tx.Rollback()
		log.Fatal("dim_service err:", err)
	}

	if _, err := tx.Exec(getSkuStm(valuesMap.skus), valuesMap.skus...); err != nil {
		tx.Rollback()
		log.Fatal("dim_sku err:", err)
	}

	if _, err := tx.Exec(getSubscriptionStm(valuesMap.subscriptions), valuesMap.subscriptions...); err != nil {
		tx.Rollback()
		log.Fatal("dim_subscription err:", err)
	}

	if _, err := tx.Exec(getUnitTypeStm(valuesMap.unitTypes), valuesMap.unitTypes...); err != nil {
		tx.Rollback()
		log.Fatal("dim_unit_type err:", err)
	}

	if _, err := tx.Exec(getUsageDateStm(valuesMap.usageDates), valuesMap.usageDates...); err != nil {
		tx.Rollback()
		log.Fatal("dim_usage_dates err:", err)
	}

	if _, err := tx.Exec(getBillingCurrencyStm(valuesMap.billingCurrencies), valuesMap.billingCurrencies...); err != nil {
		tx.Rollback()
		log.Fatal("dim_billing_currencies err:", err)
	}

	if _, err := tx.Exec(getPricingCurrencyStm(valuesMap.pricingCurrencies), valuesMap.pricingCurrencies...); err != nil {
		tx.Rollback()
		log.Fatal("dim_pricing_currencies err:", err)
	}

	if _, err := tx.Exec(getFactChargesStm(valuesMap.factCharges), valuesMap.factCharges...); err != nil {
		tx.Rollback()
		log.Fatal("fact_charge err:", err)
	}

	// Commit da transação
	if err := tx.Commit(); err != nil {
		log.Fatal("Err on commit:", err)
	}
}
