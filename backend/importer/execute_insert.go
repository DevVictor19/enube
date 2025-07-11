package importer

import (
	"log"
	"sync"

	"github.com/DevVictor19/enube/backend/importer/database"
)

func executeInsert(data *insertData, wg *sync.WaitGroup) {
	db, err := database.Get()
	if err != nil {
		log.Fatal(err)
	}

	tx, err := db.Begin()
	if err != nil {
		log.Fatal(err)
	}

	if _, err := tx.Exec(data.availabilitiesStm, data.availabilitiesValues...); err != nil {
		tx.Rollback()
		log.Fatal("dim_availabilities err:", err)
	}

	if _, err := tx.Exec(data.benefitOrderStm, data.benefitOrderValues...); err != nil {
		tx.Rollback()
		log.Fatal("dim_benefit_order err:", err)
	}

	if _, err := tx.Exec(data.benefitStm, data.benefitValues...); err != nil {
		tx.Rollback()
		log.Fatal("dim_benefit err:", err)
	}

	if _, err := tx.Exec(data.chargeTypeStm, data.chargeTypeValues...); err != nil {
		tx.Rollback()
		log.Fatal("dim_charge_type err:", err)
	}

	if _, err := tx.Exec(data.customersStm, data.customersValues...); err != nil {
		tx.Rollback()
		log.Fatal("dim_customers err:", err)
	}

	if _, err := tx.Exec(data.entitlementStm, data.entitlementValues...); err != nil {
		tx.Rollback()
		log.Fatal("dim_entitlement err:", err)
	}

	if _, err := tx.Exec(data.metersStm, data.metersValues...); err != nil {
		tx.Rollback()
		log.Fatal("dim_meters err:", err)
	}

	if _, err := tx.Exec(data.monthsChargeDateStm, data.monthsChargeDateValues...); err != nil {
		tx.Rollback()
		log.Fatal("dim_months_charge_date err:", err)
	}

	if _, err := tx.Exec(data.partnerCreditStm, data.partnerCreditValues...); err != nil {
		tx.Rollback()
		log.Fatal("dim_partner_credit err:", err)
	}

	if _, err := tx.Exec(data.partnerStm, data.partnerValues...); err != nil {
		tx.Rollback()
		log.Fatal("dim_partner err:", err)
	}

	if _, err := tx.Exec(data.productStm, data.productValues...); err != nil {
		tx.Rollback()
		log.Fatal("dim_product err:", err)
	}

	if _, err := tx.Exec(data.publisherStm, data.publisherValues...); err != nil {
		tx.Rollback()
		log.Fatal("dim_publisher err:", err)
	}

	if _, err := tx.Exec(data.resourceGroupStm, data.resourceGroupValues...); err != nil {
		tx.Rollback()
		log.Fatal("dim_resource_group err:", err)
	}

	if _, err := tx.Exec(data.resourceLocationStm, data.resourceLocationValues...); err != nil {
		tx.Rollback()
		log.Fatal("dim_resource_location err:", err)
	}

	if _, err := tx.Exec(data.serviceStm, data.serviceValues...); err != nil {
		tx.Rollback()
		log.Fatal("dim_service err:", err)
	}

	if _, err := tx.Exec(data.skuStm, data.skuValues...); err != nil {
		tx.Rollback()
		log.Fatal("dim_sku err:", err)
	}

	if _, err := tx.Exec(data.subscriptionStm, data.subscriptionValues...); err != nil {
		tx.Rollback()
		log.Fatal("dim_subscription err:", err)
	}

	if _, err := tx.Exec(data.unitTypeStm, data.unitTypeValues...); err != nil {
		tx.Rollback()
		log.Fatal("dim_unit_type err:", err)
	}

	if _, err := tx.Exec(data.chargeStm, data.chargeValues...); err != nil {
		tx.Rollback()
		log.Fatal("fact_charge err:", err)
	}

	// Commit da transação
	if err := tx.Commit(); err != nil {
		log.Fatal("Err on commit:", err)
	}

	wg.Done()
}

func prepareInsert() *insertData {
	data := insertData{
		availabilitiesStm:      getAvailabilityStm(),
		availabilitiesValues:   availabilityValues,
		benefitOrderStm:        getBenefitOrderStm(),
		benefitOrderValues:     benefitOrderValues,
		benefitStm:             getBenefitStm(),
		benefitValues:          benefitValues,
		chargeTypeStm:          getChargeTypeStm(),
		chargeTypeValues:       chargeTypeValues,
		customersStm:           getCustomerStm(),
		customersValues:        customerValues,
		entitlementStm:         getEntitlementStm(),
		entitlementValues:      entitlementValues,
		metersStm:              getMeterStm(),
		metersValues:           meterValues,
		monthsChargeDateStm:    getMonthsChargeDateStm(),
		monthsChargeDateValues: monthsChargeDateValues,
		partnerCreditStm:       getPartnerCreditStm(),
		partnerCreditValues:    partnerCreditValues,
		partnerStm:             getPartnerStm(),
		partnerValues:          partnerValues,
		productStm:             getProductStm(),
		productValues:          productValues,
		publisherStm:           getPublisherStm(),
		publisherValues:        publisherValues,
		resourceGroupStm:       getResourceGroupStm(),
		resourceGroupValues:    resourceGroupValues,
		resourceLocationStm:    getResourceLocationStm(),
		resourceLocationValues: resourceLocationValues,
		serviceStm:             getServiceStm(),
		serviceValues:          serviceValues,
		skuStm:                 getSkuStm(),
		skuValues:              skuValues,
		subscriptionStm:        getSubscriptionStm(),
		subscriptionValues:     subscriptionValues,
		unitTypeStm:            getUnitTypeStm(),
		unitTypeValues:         unitTypeValues,
		chargeStm:              getFactChargesStm(),
		chargeValues:           chargeValues,
	}
	resetValues()
	return &data
}

func resetValues() {
	resetAvailabilityValues()
	resetBenefitOrderValues()
	resetBenefitValues()
	resetChargeTypeValues()
	resetCustomerValues()
	resetEntitlementValues()
	resetMeterValues()
	resetMonthsChargeDateValues()
	resetPartnerCreditValues()
	resetPartnerValues()
	resetProductValues()
	resetPublisherValues()
	resetResourceGroupValues()
	resetResourceLocationValues()
	resetServiceValues()
	resetSkuValues()
	resetSubscriptionValues()
	resetUnitTypeValues()
	resetChargeValues()
}

type insertData struct {
	availabilitiesStm      string
	availabilitiesValues   []any
	benefitOrderStm        string
	benefitOrderValues     []any
	benefitStm             string
	benefitValues          []any
	chargeTypeStm          string
	chargeTypeValues       []any
	customersStm           string
	customersValues        []any
	entitlementStm         string
	entitlementValues      []any
	metersStm              string
	metersValues           []any
	monthsChargeDateStm    string
	monthsChargeDateValues []any
	partnerCreditStm       string
	partnerCreditValues    []any
	partnerStm             string
	partnerValues          []any
	productStm             string
	productValues          []any
	publisherStm           string
	publisherValues        []any
	resourceGroupStm       string
	resourceGroupValues    []any
	resourceLocationStm    string
	resourceLocationValues []any
	serviceStm             string
	serviceValues          []any
	skuStm                 string
	skuValues              []any
	subscriptionStm        string
	subscriptionValues     []any
	unitTypeStm            string
	unitTypeValues         []any
	chargeStm              string
	chargeValues           []any
}
