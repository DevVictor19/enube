package repositories

type Partner struct {
	SK            int    `json:"sk"`
	ID            string `json:"id"`
	Name          string `json:"name"`
	MpnID         int    `json:"mpn_id"`
	InvoiceNumber string `json:"invoice_number"`
}
