package repositories

import (
	"context"
	"database/sql"
	"time"

	"github.com/DevVictor19/enube/backend/server/types"
	"github.com/DevVictor19/enube/backend/server/utils"
)

type UsageDateRepository struct {
	db           *sql.DB
	queryTimeout time.Duration
}

func NewUsageDateRepository(db *sql.DB, qt time.Duration) *UsageDateRepository {
	return &UsageDateRepository{
		db:           db,
		queryTimeout: qt,
	}
}

func (ctl *UsageDateRepository) FindPaginated(
	ctx context.Context,
	pagination types.PaginationParams,
) (*types.PaginatedResult[UsageDate], error) {

	offset, limit := utils.GetOffsetAndLimit(pagination)

	const query = `
		SELECT 
			usage_date_sk, 
			usage_date
		FROM dim_usage_dates
		ORDER BY usage_date_sk
		LIMIT $1 OFFSET $2;
	`

	params := []any{limit, offset}

	rows, err := ctl.db.QueryContext(ctx, query, params...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	dates := []UsageDate{}
	for rows.Next() {
		var u UsageDate
		if err := rows.Scan(
			&u.SK,
			&u.UsageDate,
		); err != nil {
			return nil, err
		}
		dates = append(dates, u)
	}

	if err := rows.Err(); err != nil {
		return nil, err
	}

	var total int
	const countQuery = `SELECT COUNT(usage_date_sk) FROM dim_usage_dates`
	if err := ctl.db.QueryRowContext(ctx, countQuery).Scan(&total); err != nil {
		return nil, err
	}

	return &types.PaginatedResult[UsageDate]{
		Page:    pagination.Page,
		Limit:   pagination.Limit,
		Results: len(dates),
		Total:   total,
		Data:    dates,
	}, nil
}
