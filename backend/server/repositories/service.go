package repositories

import (
	"context"
	"database/sql"
	"time"

	"github.com/DevVictor19/enube/backend/server/types"
	"github.com/DevVictor19/enube/backend/server/utils"
)

type ServiceRepository struct {
	db           *sql.DB
	queryTimeout time.Duration
}

func NewServiceRepository(db *sql.DB, qt time.Duration) *ServiceRepository {
	return &ServiceRepository{
		db:           db,
		queryTimeout: qt,
	}
}

func (ctl *ServiceRepository) FindPaginated(
	ctx context.Context,
	pagination types.PaginationParams,
) (*types.PaginatedResult[Service], error) {

	offset, limit := utils.GetOffsetAndLimit(pagination)

	const query = `
		SELECT 
			service_sk, 
			service
		FROM dim_services
		ORDER BY service_sk
		LIMIT $1 OFFSET $2;
	`

	params := []any{limit, offset}

	rows, err := ctl.db.QueryContext(ctx, query, params...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	services := []Service{}
	for rows.Next() {
		var s Service
		if err := rows.Scan(
			&s.SK,
			&s.Service,
		); err != nil {
			return nil, err
		}
		services = append(services, s)
	}

	if err := rows.Err(); err != nil {
		return nil, err
	}

	var total int
	const countQuery = `SELECT COUNT(service_sk) FROM dim_services`
	if err := ctl.db.QueryRowContext(ctx, countQuery).Scan(&total); err != nil {
		return nil, err
	}

	return &types.PaginatedResult[Service]{
		Page:    pagination.Page,
		Limit:   pagination.Limit,
		Results: len(services),
		Total:   total,
		Data:    services,
	}, nil
}
