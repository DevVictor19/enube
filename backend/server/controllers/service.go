package controllers

import (
	"net/http"

	"github.com/DevVictor19/enube/backend/server/repositories"
	"github.com/DevVictor19/enube/backend/server/utils"
)

type ServiceController struct {
	serviceRepository *repositories.ServiceRepository
}

func NewServiceController(repo *repositories.ServiceRepository) *ServiceController {
	return &ServiceController{serviceRepository: repo}
}

func (ctl *ServiceController) FindPaginated(w http.ResponseWriter, r *http.Request) {
	pagination := utils.ParsePaginationParams(r)

	resp, err := ctl.serviceRepository.FindPaginated(r.Context(), pagination)
	if err != nil {
		utils.WriteJSON(w, http.StatusInternalServerError, map[string]string{"message": err.Error()})
		return
	}

	utils.WriteJSON(w, http.StatusOK, resp)
}
