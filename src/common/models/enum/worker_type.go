package enum

type WorkerType string

const (
	FilterWorker     WorkerType = "filter"
	GroupbyWorker    WorkerType = "groupby"
	ReducerWorker    WorkerType = "reducer"
	JoinerWorker     WorkerType = "joiner"
	AggregatorWorker WorkerType = "aggregator"
)
