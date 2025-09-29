package handler

// TODO: review this config struct and see if in the future when adding more tasks,
// this should be replaced with a more generic approach
type TaskConfig struct {
	FilterYearFrom       int
	FilterYearTo         int
	BusinessHourFrom     int
	BusinessHourTo       int
	TotalAmountThreshold float64
}
