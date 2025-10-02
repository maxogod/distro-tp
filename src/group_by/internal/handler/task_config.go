package handler

import "fmt"

// TODO: review this config struct and see if in the future when adding more tasks,
// this should be replaced with a more generic approach
type TaskConfig struct {
	FilterYearFrom       int
	FilterYearTo         int
	BusinessHourFrom     int
	BusinessHourTo       int
	TotalAmountThreshold float64
}

func (tc TaskConfig) String() string {
	return fmt.Sprintf(
		"FilterYearFrom: %d | FilterYearTo: %d | BusinessHourFrom: %d | BusinessHourTo: %d | TotalAmountThreshold: %.2f",
		tc.FilterYearFrom,
		tc.FilterYearTo,
		tc.BusinessHourFrom,
		tc.BusinessHourTo,
		tc.TotalAmountThreshold,
	)
}
