package importer

var (
	partnerCreditSequence = 0
	partnerCredits        map[string]int
	partnerCreditValues   []any
)

func getPartnerCreditSk(row []string) int {
	if partnerCredits == nil {
		partnerCredits = make(map[string]int)
	}

	creditType := row[creditTypeIndex]
	percentage := row[creditPercentageIndex]
	partnerEarnedPercentage := row[partnerEarnedCreditPercentageIndex]

	existentSequence, ok := partnerCredits[creditType]
	if !ok {
		partnerCreditSequence++
		partnerCredits[creditType] = partnerCreditSequence

		partnerCreditValues = append(
			partnerCreditValues,
			partnerCreditSequence,
			creditType,
			percentage,
			partnerEarnedPercentage,
		)

		return partnerCreditSequence
	}

	return existentSequence
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
	return buildBatchInsert(table, cols, totalVals)
}
