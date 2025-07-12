package repositories

import "time"

type ChargeData struct {
	ChargeSK               int64     `json:"charge_sk"`
	PartnerName            string    `json:"partner_name"`
	CustomerName           string    `json:"customer_name"`
	ProductName            string    `json:"product_name"`
	ResourceLocation       string    `json:"resource_location"`
	Service                string    `json:"service"`
	EffectiveUnitPrice     float64   `json:"effective_unit_price"`
	UnitPrice              float64   `json:"unit_price"`
	Quantity               float64   `json:"quantity"`
	BillingPreTaxTotal     float64   `json:"billing_pre_tax_total"`
	BillingCurrency        string    `json:"billing_currency"`
	PricingPreTaxTotal     float64   `json:"pricing_pre_tax_total"`
	PricingCurrency        string    `json:"pricing_currency"`
	PcToBcExchangeRate     float64   `json:"pc_to_bc_exchange_rate"`
	PcToBcExchangeRateDate time.Time `json:"pc_to_bc_exchange_rate_date"`
	UsageDate              time.Time `json:"usage_date"`
	ChargeStartDate        time.Time `json:"charge_start_date"`
	ChargeEndDate          time.Time `json:"charge_end_date"`
}

type ChargesResume struct {
	ChargesTotal       *int64   `json:"charges_total"`
	BillingPreTaxTotal *float64 `json:"billing_pre_tax_total"`
	PricingPreTaxTotal *float64 `json:"pricing_pre_tax_total"`
}
