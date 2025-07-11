package importer

import "database/sql"

var (
	partnerSequence = 0
	partners        map[string]int
	partnerValues   []any
)

func getPartnerSk(row []string) sql.NullInt32 {
	if len(row) <= invoiceNumberIndex {
		return sql.NullInt32{Valid: false}
	}

	partnerId := row[partnerIdIndex]
	partnerName := row[partnerNameIndex]
	mpnId := row[mpnIdIndex]
	invoiceNumber := row[invoiceNumberIndex]

	if partnerId == "" {
		return sql.NullInt32{Valid: false}
	}

	if partners == nil {
		partners = make(map[string]int)
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
			toNullableInt64(mpnId),
			invoiceNumber,
		)

		return sql.NullInt32{
			Valid: true,
			Int32: int32(partnerSequence),
		}
	}

	return sql.NullInt32{
		Valid: true,
		Int32: int32(existentSequence),
	}
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
