package controllers

import (
	"net/http"

	"github.com/DevVictor19/enube/backend/server/repositories"
	"github.com/DevVictor19/enube/backend/server/utils"
)

type ResourceLocationController struct {
	resourceLocationRepository *repositories.ResourceLocationRepository
}

func NewResourceLocationController(repo *repositories.ResourceLocationRepository) *ResourceLocationController {
	return &ResourceLocationController{resourceLocationRepository: repo}
}

func (ctl *ResourceLocationController) FindPaginated(w http.ResponseWriter, r *http.Request) {
	pagination := utils.ParsePaginationParams(r)

	resp, err := ctl.resourceLocationRepository.FindPaginated(r.Context(), pagination)
	if err != nil {
		utils.WriteJSON(w, http.StatusInternalServerError, map[string]string{"message": err.Error()})
		return
	}

	utils.WriteJSON(w, http.StatusOK, resp)
}
