package repositories

import "time"

type MonthChargeDate struct {
	SK              int       `json:"sk"`
	ChargeStartDate time.Time `json:"charge_start_date"`
	ChargeEndDate   time.Time `json:"charge_end_date"`
}
