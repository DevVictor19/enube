package utils

import (
	"net/http"
	"strconv"

	"github.com/DevVictor19/enube/backend/server/types"
)

const (
	defaultPage  = 1
	defaultLimit = 10
	maxLimit     = 100
)

func ParsePaginationParams(r *http.Request) types.PaginationParams {
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

	return types.PaginationParams{
		Page:  page,
		Limit: limit,
	}
}

func GetOffsetAndLimit(p types.PaginationParams) (int, int) {
	offset := (p.Page - 1) * p.Limit
	return offset, p.Limit
}
