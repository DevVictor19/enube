package repositories

import (
	"context"
	"database/sql"
	"fmt"
	"time"
)

type ChargeRepository struct {
	db           *sql.DB
	queryTimeout time.Duration
}

func NewChargeRepository(db *sql.DB, qt time.Duration) *ChargeRepository {
	return &ChargeRepository{
		db:           db,
		queryTimeout: qt,
	}
}

func (r *ChargeRepository) FindPaginated(
	ctx context.Context,
	pagination PaginationParams,
	filters map[string]any,
) (*FindPaginated[ChargeData], error) {
	offset, limit, err := getOffsetAndLimit(pagination)
	if err != nil {
		return nil, err
	}

	whereClause, args := buildWhereClause(filters)

	baseQuery := `
		FROM fact_charges fc
		INNER JOIN dim_customers dc ON dc.customer_sk = fc.customer_sk
		INNER JOIN dim_partners dp ON dp.partner_sk = fc.partner_sk
		INNER JOIN dim_products dp2 ON dp2.product_sk = fc.product_sk
		INNER JOIN dim_months_charge_dates dmcd ON dmcd.months_charge_date_sk = fc.months_charge_date_sk
		INNER JOIN dim_usage_dates dud ON dud.usage_date_sk = fc.usage_date_sk
		INNER JOIN dim_pricing_currencies dpc ON dpc.pricing_currency_sk = fc.pricing_currency_sk
		INNER JOIN dim_billing_currencies dbc ON dbc.billing_currency_sk = fc.billing_currency_sk
		INNER JOIN dim_resource_locations drl ON drl.resource_location_sk = fc.resource_location_sk
		INNER JOIN dim_services ds ON ds.service_sk = fc.service_sk
	`

	queryParams := []any{}

	if whereClause != "" {
		baseQuery += " WHERE " + whereClause
		queryParams = append(queryParams, args...)
	}

	queryParams = append(queryParams, limit, offset)

	query := `
		SELECT 
			fc.charge_sk,
			dp.partner_name,
			dc.customer_name,
			dp2.product_name,
			drl."location" as resource_location,
			ds.service,
			fc.effective_unit_price,
			fc.unit_price,
			fc.quantity,
			fc.billing_pre_tax_total,
			dbc.currency as billing_currency,
			fc.pricing_pre_tax_total,
			dpc.currency as pricing_currency,
			fc.pc_to_bc_exchange_rate,
			fc.pc_to_bc_exchange_rate_date,
			dud.usage_date,
			dmcd.charge_start_date,
			dmcd.charge_end_date
	` + baseQuery + `
		ORDER BY fc.charge_sk	
	`

	if whereClause != "" {
		lastParam := len(args)
		query += fmt.Sprintf(" LIMIT $%d OFFSET $%d", lastParam+1, lastParam+2)
	} else {
		query += " LIMIT $1 OFFSET $2"
	}

	rows, err := r.db.QueryContext(ctx, query, queryParams...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	charges := []ChargeData{}

	for rows.Next() {
		var c ChargeData
		err := rows.Scan(
			&c.ChargeSK,
			&c.PartnerName,
			&c.CustomerName,
			&c.ProductName,
			&c.ResourceLocation,
			&c.Service,
			&c.EffectiveUnitPrice,
			&c.UnitPrice,
			&c.Quantity,
			&c.BillingPreTaxTotal,
			&c.BillingCurrency,
			&c.PricingPreTaxTotal,
			&c.PricingCurrency,
			&c.PcToBcExchangeRate,
			&c.PcToBcExchangeRateDate,
			&c.UsageDate,
			&c.ChargeStartDate,
			&c.ChargeEndDate,
		)
		if err != nil {
			return nil, err
		}
		charges = append(charges, c)
	}

	if err := rows.Err(); err != nil {
		return nil, err
	}

	countQuery := "SELECT COUNT(fc.charge_sk) " + baseQuery

	countArgs := []any{}
	countArgs = append(countArgs, args...)

	var total int
	err = r.db.QueryRowContext(ctx, countQuery, countArgs...).Scan(&total)
	if err != nil {
		return nil, err
	}

	return &FindPaginated[ChargeData]{
		Data:    charges,
		Page:    pagination.Page,
		Limit:   pagination.Limit,
		Results: len(charges),
		Total:   total,
	}, nil
}

func (r *ChargeRepository) GetResume(ctx context.Context, filters map[string]any) (*ChargesResume, error) {
	whereClause, args := buildWhereClause(filters)

	query := `SELECT 
		COUNT(fc.charge_sk) as charges_total,
		SUM(fc.billing_pre_tax_total) as billing_pre_tax_total,
		SUM(fc.pricing_pre_tax_total) as pricing_pre_tax_total
		FROM fact_charges fc
		INNER JOIN dim_customers dc ON dc.customer_sk = fc.customer_sk
		INNER JOIN dim_partners dp ON dp.partner_sk = fc.partner_sk
		INNER JOIN dim_products dp2 ON dp2.product_sk = fc.product_sk
		INNER JOIN dim_months_charge_dates dmcd ON dmcd.months_charge_date_sk = fc.months_charge_date_sk
		INNER JOIN dim_usage_dates dud ON dud.usage_date_sk = fc.usage_date_sk
		INNER JOIN dim_pricing_currencies dpc ON dpc.pricing_currency_sk = fc.pricing_currency_sk
		INNER JOIN dim_billing_currencies dbc ON dbc.billing_currency_sk = fc.billing_currency_sk
		INNER JOIN dim_resource_locations drl ON drl.resource_location_sk = fc.resource_location_sk
		INNER JOIN dim_services ds ON ds.service_sk = fc.service_sk
	`

	queryParams := []any{}
	if whereClause != "" {
		query += " WHERE " + whereClause
		queryParams = append(queryParams, args...)
	}

	var resume ChargesResume
	err := r.db.QueryRowContext(ctx, query, queryParams...).Scan(
		&resume.ChargesTotal,
		&resume.BillingPreTaxTotal,
		&resume.PricingPreTaxTotal,
	)
	if err != nil {
		return nil, err
	}

	return &resume, nil
}
