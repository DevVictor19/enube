package importer

var (
	benefitSequence = 0
	benefits        map[string]int
	benefitValues   []any
)

func getBenefitSk(row []string) int {
	if benefits == nil {
		benefits = make(map[string]int)
	}

	benefitId := row[benefitIdIndex]
	benefitType := row[benefitTypeIndex]

	if benefitId == "" {
		return 0
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

		return benefitSequence
	}

	return existentSequence
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
