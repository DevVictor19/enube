package repositories

import "time"

type UsageDate struct {
	SK        int       `json:"sk"`
	UsageDate time.Time `json:"usage_date"`
}
