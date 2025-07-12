package controllers

import (
	"net/http"
	"strconv"

	"github.com/DevVictor19/enube/backend/server/repositories"
	"github.com/DevVictor19/enube/backend/server/utils"
)

type ChargeController struct {
	chargeRepository *repositories.ChargeRepository
}

func NewChargeController(chargeRepository *repositories.ChargeRepository) *ChargeController {
	return &ChargeController{
		chargeRepository: chargeRepository,
	}
}

func (ctl *ChargeController) FindPaginated(w http.ResponseWriter, r *http.Request) {
	pagination := parsePaginationParams(r)
	filters := ctl.getAllowedFilters(r)

	resp, err := ctl.chargeRepository.FindPaginated(r.Context(), pagination, filters)
	if err != nil {
		utils.WriteJSON(w, http.StatusInternalServerError, map[string]string{
			"message": err.Error(),
		})
		return
	}

	utils.WriteJSON(w, http.StatusOK, resp)
}

func (ctl *ChargeController) getAllowedFilters(r *http.Request) map[string]any {
	allowed := []string{
		"dc.customer_sk",
		"dp.partner_sk",
		"dp2.product_sk",
		"dmcd.months_charge_date_sk",
		"dud.usage_date_sk",
		"dbc.billing_currency_sk",
		"dpc.pricing_currency_sk",
		"drl.resource_location_sk",
		"ds.service_sk",
	}

	filters := map[string]any{}

	query := r.URL.Query()

	for _, key := range allowed {
		val := query.Get(key)

		if val == "" {
			continue
		}

		sk, err := strconv.Atoi(val)
		if err != nil {
			continue
		}

		filters[key] = sk
	}

	return filters
}
