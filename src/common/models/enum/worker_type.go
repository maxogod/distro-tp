package enum

type WorkerType string

const (
	Filter     WorkerType = "filter"
	GroupBy    WorkerType = "group_by"
	Reducer    WorkerType = "reducer"
	Joiner     WorkerType = "joiner"
	Aggregator WorkerType = "aggregator"
)
