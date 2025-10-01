package enum

type WorkerType string

const (
	Filter     WorkerType = "filter"
	GroupBy    WorkerType = "groupby"
	Reducer    WorkerType = "reducer"
	Joiner     WorkerType = "joiner"
	Aggregator WorkerType = "aggregator"
)
