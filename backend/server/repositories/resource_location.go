package repositories

import (
	"context"
	"database/sql"
	"time"

	"github.com/DevVictor19/enube/backend/server/types"
	"github.com/DevVictor19/enube/backend/server/utils"
)

type ResourceLocationRepository struct {
	db           *sql.DB
	queryTimeout time.Duration
}

func NewResourceLocationRepository(db *sql.DB, qt time.Duration) *ResourceLocationRepository {
	return &ResourceLocationRepository{
		db:           db,
		queryTimeout: qt,
	}
}

func (ctl *ResourceLocationRepository) FindPaginated(
	ctx context.Context,
	pagination types.PaginationParams,
) (*types.PaginatedResult[ResourceLocation], error) {

	offset, limit := utils.GetOffsetAndLimit(pagination)

	const query = `
		SELECT 
			resource_location_sk, 
			location
		FROM dim_resource_locations
		LIMIT $1 OFFSET $2;
	`

	params := []any{limit, offset}

	rows, err := ctl.db.QueryContext(ctx, query, params...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	locations := []ResourceLocation{}
	for rows.Next() {
		var r ResourceLocation
		if err := rows.Scan(
			&r.SK,
			&r.Location,
		); err != nil {
			return nil, err
		}
		locations = append(locations, r)
	}

	if err := rows.Err(); err != nil {
		return nil, err
	}

	var total int
	const countQuery = `SELECT COUNT(resource_location_sk) FROM dim_resource_locations`
	if err := ctl.db.QueryRowContext(ctx, countQuery).Scan(&total); err != nil {
		return nil, err
	}

	return &types.PaginatedResult[ResourceLocation]{
		Page:    pagination.Page,
		Limit:   pagination.Limit,
		Results: len(locations),
		Total:   total,
		Data:    locations,
	}, nil
}
