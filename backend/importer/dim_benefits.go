package importer

import "database/sql"

var (
	benefitSequence = 0
	benefits        map[string]int
	benefitValues   []any
)

func getBenefitSk(row []string) sql.NullInt32 {
	if len(row) <= benefitTypeIndex {
		return sql.NullInt32{Valid: false}
	}

	benefitId := row[benefitIdIndex]
	benefitType := row[benefitTypeIndex]

	if benefitId == "" {
		return sql.NullInt32{Valid: false}
	}

	if benefits == nil {
		benefits = make(map[string]int)
	}

	existentSequence, ok := benefits[benefitId]
	if !ok {
		benefitSequence++
		benefits[benefitId] = benefitSequence

		benefitValues = append(
			benefitValues,
			benefitSequence,
			benefitId,
			benefitType,
		)

		return sql.NullInt32{
			Valid: true,
			Int32: int32(benefitSequence),
		}
	}

	return sql.NullInt32{
		Valid: true,
		Int32: int32(existentSequence),
	}
}

func getBenefitStm() string {
	table := "dim_benefits"
	cols := []string{
		"benefit_sk",
		"benefit_id",
		"type",
	}
	totalVals := len(benefitValues)
	return buildBatchInsert(table, cols, totalVals)
}

func resetBenefitValues() {
	benefitValues = nil
}
