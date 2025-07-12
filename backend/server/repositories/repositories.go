package repositories

import (
	"database/sql"
	"time"
)

type Repositories struct {
	Charge           *ChargeRepository
	Customer         *CustomerRepository
	Partner          *PartnerRepository
	Product          *ProductRepository
	MonthChargeDate  *MonthChargeDateRepository
	UsageDate        *UsageDateRepository
	BillingCurrency  *BillingCurrencyRepository
	PricingCurrency  *PricingCurrencyRepository
	ResourceLocation *ResourceLocationRepository
	Service          *ServiceRepository
}

func NewRepositories(db *sql.DB, qt time.Duration) *Repositories {
	return &Repositories{
		Charge:           NewChargeRepository(db, qt),
		Customer:         NewCustomerRepository(db, qt),
		Partner:          NewPartnerRepository(db, qt),
		Product:          NewProductRepository(db, qt),
		MonthChargeDate:  NewMonthChargeDateRepository(db, qt),
		UsageDate:        NewUsageDateRepository(db, qt),
		BillingCurrency:  NewBillingCurrencyRepository(db, qt),
		PricingCurrency:  NewPricingCurrencyRepository(db, qt),
		ResourceLocation: NewResourceLocationRepository(db, qt),
		Service:          NewServiceRepository(db, qt),
	}
}
