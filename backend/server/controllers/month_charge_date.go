package controllers

import (
	"net/http"

	"github.com/DevVictor19/enube/backend/server/repositories"
	"github.com/DevVictor19/enube/backend/server/utils"
)

type MonthChargeDateController struct {
	monthChargeDateRepository *repositories.MonthChargeDateRepository
}

func NewMonthChargeDateController(repo *repositories.MonthChargeDateRepository) *MonthChargeDateController {
	return &MonthChargeDateController{monthChargeDateRepository: repo}
}

func (ctl *MonthChargeDateController) FindPaginated(w http.ResponseWriter, r *http.Request) {
	pagination := utils.ParsePaginationParams(r)

	resp, err := ctl.monthChargeDateRepository.FindPaginated(r.Context(), pagination)
	if err != nil {
		utils.WriteJSON(w, http.StatusInternalServerError, map[string]string{"message": err.Error()})
		return
	}

	utils.WriteJSON(w, http.StatusOK, resp)
}
