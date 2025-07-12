package controllers

import (
	"net/http"

	"github.com/DevVictor19/enube/backend/server/repositories"
	"github.com/DevVictor19/enube/backend/server/utils"
)

type PartnerController struct {
	partnerRepository *repositories.PartnerRepository
}

func NewPartnerController(repo *repositories.PartnerRepository) *PartnerController {
	return &PartnerController{partnerRepository: repo}
}

func (ctl *PartnerController) FindPaginated(w http.ResponseWriter, r *http.Request) {
	pagination := utils.ParsePaginationParams(r)

	resp, err := ctl.partnerRepository.FindPaginated(r.Context(), pagination)
	if err != nil {
		utils.WriteJSON(w, http.StatusInternalServerError, map[string]string{"message": err.Error()})
		return
	}

	utils.WriteJSON(w, http.StatusOK, resp)
}
