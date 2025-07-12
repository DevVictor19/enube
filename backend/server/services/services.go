package services

import "github.com/DevVictor19/enube/backend/server/env"

type Services struct {
	JWT    *JWTService
	Bcrypt *BcryptService
}

func NewServices(cfg *env.Config) *Services {
	return &Services{
		JWT:    NewJWTService(cfg.JWT.Secret, cfg.JWT.Aud, cfg.JWT.Iss),
		Bcrypt: NewBcryptService(),
	}
}
