package services

import (
	"fmt"
	"time"

	"github.com/golang-jwt/jwt/v5"
)

type JWTService struct {
	Secret string
	Aud    string
	Iss    string
}

func NewJWTService(secret, aud, iss string) *JWTService {
	return &JWTService{secret, iss, aud}
}

func (a *JWTService) GenerateToken(userId int, exp time.Duration) (string, error) {
	claims := jwt.MapClaims{
		"sub": userId,
		"exp": time.Now().Add(exp).Unix(),
		"iat": time.Now().Unix(),
		"nbf": time.Now().Unix(),
		"iss": a.Iss,
		"aud": a.Aud,
	}

	token := jwt.NewWithClaims(jwt.SigningMethodHS256, claims)

	tokenString, err := token.SignedString([]byte(a.Secret))
	if err != nil {
		return "", err
	}

	return tokenString, nil
}

func (a *JWTService) ValidateToken(token string) (*jwt.Token, error) {
	return jwt.Parse(token, func(t *jwt.Token) (any, error) {
		if _, ok := t.Method.(*jwt.SigningMethodHMAC); !ok {
			return nil, fmt.Errorf("unexpected signing method %v", t.Header["alg"])
		}

		return []byte(a.Secret), nil
	},
		jwt.WithExpirationRequired(),
		jwt.WithAudience(a.Aud),
		jwt.WithIssuer(a.Iss),
		jwt.WithValidMethods([]string{jwt.SigningMethodHS256.Name}),
	)
}
