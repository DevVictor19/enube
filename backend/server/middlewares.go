package server

import (
	"context"
	"fmt"
	"net/http"
	"strconv"
	"strings"

	"github.com/DevVictor19/enube/backend/server/repositories"
	"github.com/DevVictor19/enube/backend/server/services"
	"github.com/DevVictor19/enube/backend/server/types"
	"github.com/DevVictor19/enube/backend/server/utils"
	"github.com/golang-jwt/jwt/v5"
)

type middlewareFn func(http.Handler) http.Handler

func makeJWTAuthMiddleware(jwtSvc *services.JWTService, usrRepo *repositories.UserRepository) middlewareFn {
	return func(h http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			authHeader := r.Header.Get("Authorization")
			if authHeader == "" {
				utils.WriteJSON(w, http.StatusBadRequest, map[string]string{
					"message": "Authorization header is missing",
				})
				return
			}

			parts := strings.Split(authHeader, " ")
			if len(parts) != 2 || parts[0] != "Bearer" {
				utils.WriteJSON(w, http.StatusBadRequest, map[string]string{
					"message": "Authorization header is malformed",
				})
				return
			}

			token := parts[1]
			jwtToken, err := jwtSvc.ValidateToken(token)
			if err != nil {
				utils.WriteJSON(w, http.StatusUnauthorized, map[string]string{
					"message": "Invalid token",
				})
				return
			}

			claims, _ := jwtToken.Claims.(jwt.MapClaims)
			userId, err := strconv.ParseInt(fmt.Sprintf("%.f", claims["sub"]), 10, 32)
			if err != nil {
				utils.WriteJSON(w, http.StatusUnauthorized, map[string]string{
					"message": "Invalid token",
				})
				return

			}

			ctx := r.Context()
			user, err := usrRepo.FindByID(ctx, int(userId))
			if err != nil {
				utils.WriteJSON(w, http.StatusForbidden, map[string]string{
					"message": "User not allowed",
				})
				return
			}

			ctx = context.WithValue(ctx, types.UserKey, user)
			h.ServeHTTP(w, r.WithContext(ctx))
		})
	}
}
