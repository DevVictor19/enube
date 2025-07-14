package repositories

import (
	"context"
	"database/sql"
	"time"

	"github.com/DevVictor19/enube/backend/server/types"
	"github.com/DevVictor19/enube/backend/server/utils"
)

type PricingCurrencyRepository struct {
	db           *sql.DB
	queryTimeout time.Duration
}

func NewPricingCurrencyRepository(db *sql.DB, qt time.Duration) *PricingCurrencyRepository {
	return &PricingCurrencyRepository{
		db:           db,
		queryTimeout: qt,
	}
}

func (ctl *PricingCurrencyRepository) FindPaginated(
	ctx context.Context,
	pagination types.PaginationParams,
) (*types.PaginatedResult[PricingCurrency], error) {

	offset, limit := utils.GetOffsetAndLimit(pagination)

	const query = `
		SELECT 
			pricing_currency_sk, 
			currency
		FROM dim_pricing_currencies
		ORDER BY pricing_currency_sk
		LIMIT $1 OFFSET $2;
	`

	params := []any{limit, offset}

	rows, err := ctl.db.QueryContext(ctx, query, params...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	currencies := []PricingCurrency{}
	for rows.Next() {
		var c PricingCurrency
		if err := rows.Scan(
			&c.SK,
			&c.Currency,
		); err != nil {
			return nil, err
		}
		currencies = append(currencies, c)
	}

	if err := rows.Err(); err != nil {
		return nil, err
	}

	var total int
	const countQuery = `SELECT COUNT(pricing_currency_sk) FROM dim_pricing_currencies`
	if err := ctl.db.QueryRowContext(ctx, countQuery).Scan(&total); err != nil {
		return nil, err
	}

	return &types.PaginatedResult[PricingCurrency]{
		Page:    pagination.Page,
		Limit:   pagination.Limit,
		Results: len(currencies),
		Total:   total,
		Data:    currencies,
	}, nil
}
