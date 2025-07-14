package repositories

import (
	"context"
	"database/sql"
	"time"

	"github.com/DevVictor19/enube/backend/server/types"
	"github.com/DevVictor19/enube/backend/server/utils"
)

type MonthChargeDateRepository struct {
	db           *sql.DB
	queryTimeout time.Duration
}

func NewMonthChargeDateRepository(db *sql.DB, qt time.Duration) *MonthChargeDateRepository {
	return &MonthChargeDateRepository{
		db:           db,
		queryTimeout: qt,
	}
}

func (ctl *MonthChargeDateRepository) FindPaginated(
	ctx context.Context,
	pagination types.PaginationParams,
) (*types.PaginatedResult[MonthChargeDate], error) {

	offset, limit := utils.GetOffsetAndLimit(pagination)

	const query = `
		SELECT 
			months_charge_date_sk, 
			charge_start_date, 
			charge_end_date
		FROM dim_months_charge_dates
		ORDER BY months_charge_date_sk
		LIMIT $1 OFFSET $2;
	`

	params := []any{limit, offset}

	rows, err := ctl.db.QueryContext(ctx, query, params...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	dates := []MonthChargeDate{}
	for rows.Next() {
		var m MonthChargeDate
		if err := rows.Scan(
			&m.SK,
			&m.ChargeStartDate,
			&m.ChargeEndDate,
		); err != nil {
			return nil, err
		}
		dates = append(dates, m)
	}

	if err := rows.Err(); err != nil {
		return nil, err
	}

	var total int
	const countQuery = `SELECT COUNT(months_charge_date_sk) FROM dim_months_charge_dates`
	if err := ctl.db.QueryRowContext(ctx, countQuery).Scan(&total); err != nil {
		return nil, err
	}

	return &types.PaginatedResult[MonthChargeDate]{
		Page:    pagination.Page,
		Limit:   pagination.Limit,
		Results: len(dates),
		Total:   total,
		Data:    dates,
	}, nil
}
