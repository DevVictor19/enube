package importer

var (
	partnerSequence = 0
	partners        map[string]int
	partnerValues   []any
)

func getPartnerSk(row []string) int {
	if partners == nil {
		partners = make(map[string]int)
	}

	partnerId := row[partnerIdIndex]
	partnerName := row[partnerNameIndex]
	mpnId := row[mpnIdIndex]
	invoiceNumber := row[invoiceNumberIndex]

	if partnerId == "" {
		return 0
	}

	existentSequence, ok := partners[partnerId]
	if !ok {
		partnerSequence++
		partners[partnerId] = partnerSequence

		partnerValues = append(
			partnerValues,
			partnerSequence,
			partnerId,
			partnerName,
			mpnId,
			invoiceNumber,
		)

		return partnerSequence
	}

	return existentSequence
}

func getPartnerStm() string {
	table := "dim_partners"
	cols := []string{"partner_sk", "partner_id", "partner_name", "mpn_id", "invoice_number"}
	totalVals := len(partnerValues)
	return buildBatchInsert(table, cols, totalVals)
}

func resetPartnerValues() {
	partnerValues = nil
}
