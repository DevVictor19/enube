package controllers

import (
	"github.com/DevVictor19/enube/backend/server/repositories"
	"github.com/DevVictor19/enube/backend/server/services"
)

type Controllers struct {
	Charge           *ChargeController
	Customer         *CustomerController
	Partner          *PartnerController
	Product          *ProductController
	MonthChargeDate  *MonthChargeDateController
	UsageDate        *UsageDateController
	BillingCurrency  *BillingCurrencyController
	PricingCurrency  *PricingCurrencyController
	ResourceLocation *ResourceLocationController
	Service          *ServiceController
	Auth             *AuthController
}

func NewControllers(repos *repositories.Repositories, svcs *services.Services) *Controllers {
	return &Controllers{
		Charge:           NewChargeController(repos.Charge),
		Customer:         NewCustomerController(repos.Customer),
		Partner:          NewPartnerController(repos.Partner),
		Product:          NewProductController(repos.Product),
		MonthChargeDate:  NewMonthChargeDateController(repos.MonthChargeDate),
		UsageDate:        NewUsageDateController(repos.UsageDate),
		BillingCurrency:  NewBillingCurrencyController(repos.BillingCurrency),
		PricingCurrency:  NewPricingCurrencyController(repos.PricingCurrency),
		ResourceLocation: NewResourceLocationController(repos.ResourceLocation),
		Service:          NewServiceController(repos.Service),
		Auth:             NewAuthController(svcs.JWT, svcs.Bcrypt, repos.User),
	}
}
