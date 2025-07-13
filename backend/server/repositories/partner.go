package repositories

import (
	"context"
	"database/sql"
	"time"

	"github.com/DevVictor19/enube/backend/server/types"
	"github.com/DevVictor19/enube/backend/server/utils"
)

type PartnerRepository struct {
	db           *sql.DB
	queryTimeout time.Duration
}

func NewPartnerRepository(db *sql.DB, qt time.Duration) *PartnerRepository {
	return &PartnerRepository{
		db:           db,
		queryTimeout: qt,
	}
}

func (ctl *PartnerRepository) FindPaginated(
	ctx context.Context,
	pagination types.PaginationParams,
) (*types.PaginatedResult[Partner], error) {

	offset, limit := utils.GetOffsetAndLimit(pagination)

	const query = `
		SELECT 
			partner_sk, 
			partner_id, 
			partner_name, 
			mpn_id, 
			invoice_number
		FROM dim_partners
		LIMIT $1 OFFSET $2;
	`

	params := []any{limit, offset}

	rows, err := ctl.db.QueryContext(ctx, query, params...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var partners []Partner
	for rows.Next() {
		var p Partner
		if err := rows.Scan(
			&p.SK,
			&p.ID,
			&p.Name,
			&p.MpnID,
			&p.InvoiceNumber,
		); err != nil {
			return nil, err
		}
		partners = append(partners, p)
	}

	if err := rows.Err(); err != nil {
		return nil, err
	}

	var total int
	const countQuery = `SELECT COUNT(partner_sk) FROM dim_partners`
	if err := ctl.db.QueryRowContext(ctx, countQuery).Scan(&total); err != nil {
		return nil, err
	}

	result := &types.PaginatedResult[Partner]{
		Page:    pagination.Page,
		Limit:   pagination.Limit,
		Results: len(partners),
		Total:   total,
		Data:    partners,
	}

	return result, nil
}
