package controllers

import (
	"net/http"

	"github.com/DevVictor19/enube/backend/server/repositories"
	"github.com/DevVictor19/enube/backend/server/utils"
)

type UsageDateController struct {
	usageDateRepository *repositories.UsageDateRepository
}

func NewUsageDateController(repo *repositories.UsageDateRepository) *UsageDateController {
	return &UsageDateController{usageDateRepository: repo}
}

func (ctl *UsageDateController) FindPaginated(w http.ResponseWriter, r *http.Request) {
	pagination := utils.ParsePaginationParams(r)

	resp, err := ctl.usageDateRepository.FindPaginated(r.Context(), pagination)
	if err != nil {
		utils.WriteJSON(w, http.StatusInternalServerError, map[string]string{"message": err.Error()})
		return
	}

	utils.WriteJSON(w, http.StatusOK, resp)
}
