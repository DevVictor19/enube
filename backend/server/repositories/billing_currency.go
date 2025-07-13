package repositories

import (
	"context"
	"database/sql"
	"time"

	"github.com/DevVictor19/enube/backend/server/types"
	"github.com/DevVictor19/enube/backend/server/utils"
)

type BillingCurrencyRepository struct {
	db           *sql.DB
	queryTimeout time.Duration
}

func NewBillingCurrencyRepository(db *sql.DB, qt time.Duration) *BillingCurrencyRepository {
	return &BillingCurrencyRepository{
		db:           db,
		queryTimeout: qt,
	}
}

func (ctl *BillingCurrencyRepository) FindPaginated(
	ctx context.Context,
	pagination types.PaginationParams,
) (*types.PaginatedResult[BillingCurrency], error) {

	offset, limit := utils.GetOffsetAndLimit(pagination)

	const query = `
		SELECT 
			billing_currency_sk, 
			currency
		FROM dim_billing_currencies
		LIMIT $1 OFFSET $2;
	`

	params := []any{limit, offset}

	rows, err := ctl.db.QueryContext(ctx, query, params...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	currencies := []BillingCurrency{}
	for rows.Next() {
		var c BillingCurrency
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
	const countQuery = `SELECT COUNT(billing_currency_sk) FROM dim_billing_currencies`
	if err := ctl.db.QueryRowContext(ctx, countQuery).Scan(&total); err != nil {
		return nil, err
	}

	return &types.PaginatedResult[BillingCurrency]{
		Page:    pagination.Page,
		Limit:   pagination.Limit,
		Results: len(currencies),
		Total:   total,
		Data:    currencies,
	}, nil
}
