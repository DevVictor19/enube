package importer

import (
	"database/sql"

	"github.com/DevVictor19/enube/backend/importer/helpers"
)

var (
	partnerCreditSequence = 0
	partnerCredits        map[string]int
	partnerCreditValues   []any
)

func getPartnerCreditSk(row []string) sql.NullInt32 {
	if len(row) <= partnerEarnedCreditPercentageIndex {
		return sql.NullInt32{Valid: false}
	}

	creditType := row[creditTypeIndex]
	percentage := row[creditPercentageIndex]
	partnerEarnedPercentage := row[partnerEarnedCreditPercentageIndex]

	if creditType == "" {
		return sql.NullInt32{Valid: false}
	}

	if partnerCredits == nil {
		partnerCredits = make(map[string]int)
	}

	existentSequence, ok := partnerCredits[creditType]
	if !ok {
		partnerCreditSequence++
		partnerCredits[creditType] = partnerCreditSequence

		partnerCreditValues = append(
			partnerCreditValues,
			partnerCreditSequence,
			creditType,
			helpers.ToNullableFloat64(percentage),
			helpers.ToNullableFloat64(partnerEarnedPercentage),
		)

		return sql.NullInt32{
			Valid: true,
			Int32: int32(partnerCreditSequence),
		}
	}

	return sql.NullInt32{
		Valid: true,
		Int32: int32(existentSequence),
	}
}

func getPartnerCreditStm() string {
	table := "dim_partner_credits"
	cols := []string{
		"partner_credit_sk",
		"type",
		"percentage",
		"partner_earned_percentage",
	}
	totalVals := len(partnerCreditValues)
	return helpers.BuildBatchInsert(table, cols, totalVals)
}

func resetPartnerCreditValues() {
	partnerCreditValues = nil
}
