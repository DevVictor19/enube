package controllers

import (
	"net/http"
	"strconv"

	"github.com/DevVictor19/enube/backend/server/repositories"
)

const (
	defaultPage  = 1
	defaultLimit = 10
	maxLimit     = 100
)

func parsePaginationParams(r *http.Request) repositories.PaginationParams {
	query := r.URL.Query()

	page, err := strconv.Atoi(query.Get("page"))
	if err != nil || page < 1 {
		page = defaultPage
	}

	limit, err := strconv.Atoi(query.Get("limit"))
	if err != nil || limit < 1 {
		limit = defaultLimit
	} else if limit > maxLimit {
		limit = maxLimit
	}

	return repositories.PaginationParams{
		Page:  page,
		Limit: limit,
	}
}
