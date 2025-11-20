package enum

type WorkerType string

const (
	Gateway          WorkerType = "gateway"
	Controller       WorkerType = "controller"
	FilterWorker     WorkerType = "filter"
	GroupbyWorker    WorkerType = "groupby"
	ReducerWorker    WorkerType = "reducer"
	JoinerWorker     WorkerType = "joiner"
	AggregatorWorker WorkerType = "aggregator"
	None             WorkerType = "none"
)
