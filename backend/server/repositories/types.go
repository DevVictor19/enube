package repositories

type FindPaginated[T any] struct {
	Page    int `json:"page"`
	Limit   int `json:"limit"`
	Results int `json:"results"`
	Total   int `json:"total"`
	Data    []T `json:"data"`
}

type PaginationParams struct {
	Page  int
	Limit int
}
