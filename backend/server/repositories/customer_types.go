package repositories

type Customer struct {
	SK         int    `json:"customer_sk"`
	ID         string `json:"customer_id"`
	Name       string `json:"customer_name"`
	DomainName string `json:"customer_domain_name"`
	Country    string `json:"customer_country"`
	Tier2MpnID int    `json:"tier_2_mpn_id"`
}
