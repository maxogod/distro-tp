package enum

type WorkerType string

const (
	Filter       WorkerType = "filter"
	GroupBy      WorkerType = "group_by"
	ReducerSum   WorkerType = "reducer_sum"
	ReducerCount WorkerType = "reducer_count"
	Joiner       WorkerType = "joiner"
	Aggregator   WorkerType = "aggregator"
)
