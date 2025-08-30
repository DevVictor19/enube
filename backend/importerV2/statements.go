package importerV2

import "github.com/DevVictor19/enube/backend/importerV2/helpers"

func getBenefitOrderStm(values []any) string {
	table := "dim_benefit_orders"
	cols := []string{
		"benefit_order_sk",
		"benefit_order_id",
	}
	return helpers.BuildBatchInsert(table, cols, len(values))
}

func getAvailabilityStm(values []any) string {
	table := "dim_availabilities"
	cols := []string{
		"availability_sk",
		"availability_id",
	}
	return helpers.BuildBatchInsert(table, cols, len(values))
}

func getBenefitStm(values []any) string {
	table := "dim_benefits"
	cols := []string{
		"benefit_sk",
		"benefit_id",
		"type",
	}
	return helpers.BuildBatchInsert(table, cols, len(values))
}

func getBillingCurrencyStm(values []any) string {
	table := "dim_billing_currencies"
	cols := []string{
		"billing_currency_sk",
		"currency",
	}
	return helpers.BuildBatchInsert(table, cols, len(values))
}

func getChargeTypeStm(values []any) string {
	table := "dim_charge_types"
	cols := []string{
		"charge_type_sk",
		"type",
	}
	return helpers.BuildBatchInsert(table, cols, len(values))
}

func getCustomerStm(values []any) string {
	table := "dim_customers"
	cols := []string{
		"customer_sk",
		"customer_id",
		"customer_name",
		"customer_domain_name",
		"customer_country",
		"tier_2_mpn_id",
	}
	return helpers.BuildBatchInsert(table, cols, len(values))
}

func getEntitlementStm(values []any) string {
	table := "dim_entitlements"
	cols := []string{
		"entitlement_sk",
		"entitlement_id",
		"description",
	}
	return helpers.BuildBatchInsert(table, cols, len(values))
}

func getMeterStm(values []any) string {
	table := "dim_meters"
	cols := []string{
		"meter_sk",
		"meter_id",
		"name",
		"category",
		"type",
		"subcategory",
		"region",
		"unit",
	}
	return helpers.BuildBatchInsert(table, cols, len(values))
}

func getMonthsChargeDateStm(values []any) string {
	table := "dim_months_charge_dates"
	cols := []string{
		"months_charge_date_sk",
		"charge_start_date",
		"charge_end_date",
	}
	return helpers.BuildBatchInsert(table, cols, len(values))
}

func getPartnerCreditStm(values []any) string {
	table := "dim_partner_credits"
	cols := []string{
		"partner_credit_sk",
		"type",
		"percentage",
		"partner_earned_percentage",
	}
	return helpers.BuildBatchInsert(table, cols, len(values))
}

func getPartnerStm(values []any) string {
	table := "dim_partners"
	cols := []string{"partner_sk", "partner_id", "partner_name", "mpn_id", "invoice_number"}
	return helpers.BuildBatchInsert(table, cols, len(values))
}

func getPricingCurrencyStm(values []any) string {
	table := "dim_pricing_currencies"
	cols := []string{
		"pricing_currency_sk",
		"currency",
	}
	return helpers.BuildBatchInsert(table, cols, len(values))
}

func getProductStm(values []any) string {
	table := "dim_products"
	cols := []string{
		"product_sk",
		"product_id",
		"product_name",
	}
	return helpers.BuildBatchInsert(table, cols, len(values))
}

func getPublisherStm(values []any) string {
	table := "dim_publishers"
	cols := []string{
		"publisher_sk",
		"publisher_id",
		"publisher_name",
	}
	return helpers.BuildBatchInsert(table, cols, len(values))
}

func getResourceGroupStm(values []any) string {
	table := "dim_resource_groups"
	cols := []string{
		"resource_group_sk",
		"name",
	}
	return helpers.BuildBatchInsert(table, cols, len(values))
}

func getResourceLocationStm(values []any) string {
	table := "dim_resource_locations"
	cols := []string{
		"resource_location_sk",
		"location",
	}
	return helpers.BuildBatchInsert(table, cols, len(values))
}

func getServiceStm(values []any) string {
	table := "dim_services"
	cols := []string{
		"service_sk",
		"service",
	}
	return helpers.BuildBatchInsert(table, cols, len(values))
}

func getSkuStm(values []any) string {
	table := "dim_skus"
	cols := []string{
		"sku_sk",
		"sku_id",
		"sku_name",
	}
	return helpers.BuildBatchInsert(table, cols, len(values))
}

func getSubscriptionStm(values []any) string {
	table := "dim_subscriptions"
	cols := []string{
		"subscription_sk",
		"subscription_id",
		"description",
	}
	return helpers.BuildBatchInsert(table, cols, len(values))
}

func getUnitTypeStm(values []any) string {
	table := "dim_unit_types"
	cols := []string{
		"unit_type_sk",
		"type",
	}
	return helpers.BuildBatchInsert(table, cols, len(values))
}

func getUsageDateStm(values []any) string {
	table := "dim_usage_dates"
	cols := []string{
		"usage_date_sk",
		"usage_date",
	}
	return helpers.BuildBatchInsert(table, cols, len(values))
}

func getFactChargesStm(values []any) string {
	table := "fact_charges"
	cols := []string{
		"partner_sk",
		"months_charge_date_sk",
		"customer_sk",
		"meter_sk",
		"product_sk",
		"sku_sk",
		"publisher_sk",
		"subscription_sk",
		"resource_location_sk",
		"resource_group_sk",
		"service_sk",
		"charge_type_sk",
		"unit_type_sk",
		"entitlement_sk",
		"partner_credit_sk",
		"benefit_sk",
		"benefit_order_sk",
		"availability_sk",
		"usage_date_sk",
		"billing_currency_sk",
		"pricing_currency_sk",
		"resource_uri",
		"effective_unit_price",
		"unit_price",
		"quantity",
		"billing_pre_tax_total",
		"pricing_pre_tax_total",
		"pc_to_bc_exchange_rate",
		"pc_to_bc_exchange_rate_date",
		"service_info_1",
		"service_info_2",
		"tags",
		"additional_info",
	}
	return helpers.BuildBatchInsert(table, cols, len(values))
}
