package controllers

import (
	"net/http"

	"github.com/DevVictor19/enube/backend/server/repositories"
	"github.com/DevVictor19/enube/backend/server/utils"
)

type BillingCurrencyController struct {
	billingCurrencyRepository *repositories.BillingCurrencyRepository
}

func NewBillingCurrencyController(repo *repositories.BillingCurrencyRepository) *BillingCurrencyController {
	return &BillingCurrencyController{billingCurrencyRepository: repo}
}

func (ctl *BillingCurrencyController) FindPaginated(w http.ResponseWriter, r *http.Request) {
	pagination := utils.ParsePaginationParams(r)

	resp, err := ctl.billingCurrencyRepository.FindPaginated(r.Context(), pagination)
	if err != nil {
		utils.WriteJSON(w, http.StatusInternalServerError, map[string]string{"message": err.Error()})
		return
	}

	utils.WriteJSON(w, http.StatusOK, resp)
}
