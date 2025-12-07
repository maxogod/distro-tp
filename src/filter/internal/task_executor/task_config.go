package task_executor

import "fmt"

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
