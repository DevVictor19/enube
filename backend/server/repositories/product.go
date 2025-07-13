package repositories

import (
	"context"
	"database/sql"
	"time"

	"github.com/DevVictor19/enube/backend/server/types"
	"github.com/DevVictor19/enube/backend/server/utils"
)

type ProductRepository struct {
	db           *sql.DB
	queryTimeout time.Duration
}

func NewProductRepository(db *sql.DB, qt time.Duration) *ProductRepository {
	return &ProductRepository{
		db:           db,
		queryTimeout: qt,
	}
}

func (ctl *ProductRepository) FindPaginated(
	ctx context.Context,
	pagination types.PaginationParams,
) (*types.PaginatedResult[Product], error) {

	offset, limit := utils.GetOffsetAndLimit(pagination)

	const query = `
		SELECT 
			product_sk, 
			product_id, 
			product_name
		FROM dim_products
		LIMIT $1 OFFSET $2;
	`

	params := []any{limit, offset}

	rows, err := ctl.db.QueryContext(ctx, query, params...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	products := []Product{}
	for rows.Next() {
		var p Product
		if err := rows.Scan(
			&p.SK,
			&p.ID,
			&p.Name,
		); err != nil {
			return nil, err
		}
		products = append(products, p)
	}

	if err := rows.Err(); err != nil {
		return nil, err
	}

	var total int
	const countQuery = `SELECT COUNT(product_sk) FROM dim_products`
	if err := ctl.db.QueryRowContext(ctx, countQuery).Scan(&total); err != nil {
		return nil, err
	}

	result := &types.PaginatedResult[Product]{
		Page:    pagination.Page,
		Limit:   pagination.Limit,
		Results: len(products),
		Total:   total,
		Data:    products,
	}

	return result, nil
}
