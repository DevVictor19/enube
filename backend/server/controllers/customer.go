package controllers

import (
	"net/http"

	"github.com/DevVictor19/enube/backend/server/repositories"
	"github.com/DevVictor19/enube/backend/server/utils"
)

type CustomerController struct {
	customerRepository *repositories.CustomerRepository
}

func NewCustomerController(repo *repositories.CustomerRepository) *CustomerController {
	return &CustomerController{
		customerRepository: repo,
	}
}

func (ctl *CustomerController) FindPaginated(w http.ResponseWriter, r *http.Request) {
	pagination := utils.ParsePaginationParams(r)

	resp, err := ctl.customerRepository.FindPaginated(r.Context(), pagination)
	if err != nil {
		utils.WriteJSON(w, http.StatusInternalServerError, map[string]string{
			"message": err.Error(),
		})
		return
	}

	utils.WriteJSON(w, http.StatusOK, resp)
}
