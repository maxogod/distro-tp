package enum

type WorkerType string

const (
	Gateway          WorkerType = "gateway"
	FilterWorker     WorkerType = "filter"
	GroupbyWorker    WorkerType = "groupby"
	ReducerWorker    WorkerType = "reducer"
	JoinerWorker     WorkerType = "joiner"
	AggregatorWorker WorkerType = "aggregator"
	None             WorkerType = "none"
)
