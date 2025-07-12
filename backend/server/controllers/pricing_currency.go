package controllers

import (
	"net/http"

	"github.com/DevVictor19/enube/backend/server/repositories"
	"github.com/DevVictor19/enube/backend/server/utils"
)

type PricingCurrencyController struct {
	pricingCurrencyRepository *repositories.PricingCurrencyRepository
}

func NewPricingCurrencyController(repo *repositories.PricingCurrencyRepository) *PricingCurrencyController {
	return &PricingCurrencyController{pricingCurrencyRepository: repo}
}

func (ctl *PricingCurrencyController) FindPaginated(w http.ResponseWriter, r *http.Request) {
	pagination := utils.ParsePaginationParams(r)

	resp, err := ctl.pricingCurrencyRepository.FindPaginated(r.Context(), pagination)
	if err != nil {
		utils.WriteJSON(w, http.StatusInternalServerError, map[string]string{"message": err.Error()})
		return
	}

	utils.WriteJSON(w, http.StatusOK, resp)
}
