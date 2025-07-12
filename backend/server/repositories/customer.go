package repositories

import (
	"context"
	"database/sql"
	"time"

	"github.com/DevVictor19/enube/backend/server/types"
	"github.com/DevVictor19/enube/backend/server/utils"
)

type CustomerRepository struct {
	db           *sql.DB
	queryTimeout time.Duration
}

func NewCustomerRepository(db *sql.DB, qt time.Duration) *CustomerRepository {
	return &CustomerRepository{
		db:           db,
		queryTimeout: qt,
	}
}

func (ctl *CustomerRepository) FindPaginated(
	ctx context.Context,
	pagination types.PaginationParams,
) (*types.PaginatedResult[Customer], error) {

	offset, limit, err := utils.GetOffsetAndLimit(pagination)
	if err != nil {
		return nil, err
	}

	const query = `
		SELECT 
			customer_sk, 
			customer_id, 
			customer_name, 
			customer_domain_name, 
			customer_country, 
			tier_2_mpn_id
		FROM dim_customers
		LIMIT $1 OFFSET $2;
	`

	params := []any{limit, offset}

	rows, err := ctl.db.QueryContext(ctx, query, params...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var customers []Customer
	for rows.Next() {
		var c Customer
		if err := rows.Scan(
			&c.SK,
			&c.ID,
			&c.Name,
			&c.DomainName,
			&c.Country,
			&c.Tier2MpnID,
		); err != nil {
			return nil, err
		}
		customers = append(customers, c)
	}

	if err := rows.Err(); err != nil {
		return nil, err
	}

	var total int
	const countQuery = `SELECT COUNT(customer_sk) FROM dim_customers`
	if err := ctl.db.QueryRowContext(ctx, countQuery).Scan(&total); err != nil {
		return nil, err
	}

	result := &types.PaginatedResult[Customer]{
		Page:    pagination.Page,
		Limit:   pagination.Limit,
		Results: len(customers),
		Total:   total,
		Data:    customers,
	}

	return result, nil
}
