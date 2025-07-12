package controllers

import (
	"net/http"
	"time"

	"github.com/DevVictor19/enube/backend/server/controllers/dtos"
	"github.com/DevVictor19/enube/backend/server/repositories"
	"github.com/DevVictor19/enube/backend/server/services"
	"github.com/DevVictor19/enube/backend/server/utils"
)

type AuthController struct {
	jwtService     *services.JWTService
	bcryptService  *services.BcryptService
	userRepository *repositories.UserRepository
}

func NewAuthController(
	jwtSvc *services.JWTService,
	bcryptSvc *services.BcryptService,
	usrRepo *repositories.UserRepository) *AuthController {
	return &AuthController{
		jwtService:     jwtSvc,
		bcryptService:  bcryptSvc,
		userRepository: usrRepo,
	}
}

func (ctl *AuthController) Login(w http.ResponseWriter, r *http.Request) {
	var body dtos.LoginDTO
	if err := utils.ReadJSON(w, r, &body); err != nil {
		utils.WriteJSON(w, http.StatusInternalServerError, map[string]string{
			"message": err.Error(),
		})
		return
	}

	ctx := r.Context()

	user, err := ctl.userRepository.FindByEmail(ctx, body.Email)
	if err != nil {
		utils.WriteJSON(w, http.StatusInternalServerError, map[string]string{
			"message": err.Error(),
		})
		return
	}

	if user == nil {
		utils.WriteJSON(w, http.StatusUnauthorized, map[string]string{
			"message": "Invalid email of password",
		})
		return
	}

	valid := ctl.bcryptService.Compare(body.Password, user.Password)

	if !valid {
		utils.WriteJSON(w, http.StatusUnauthorized, map[string]string{
			"message": "Invalid email of password",
		})
		return
	}

	token, err := ctl.jwtService.GenerateToken(user.ID, time.Hour)
	if err != nil {
		utils.WriteJSON(w, http.StatusInternalServerError, map[string]string{
			"message": err.Error(),
		})
		return
	}

	utils.WriteJSON(w, http.StatusOK, map[string]string{
		"token": token,
	})
}
