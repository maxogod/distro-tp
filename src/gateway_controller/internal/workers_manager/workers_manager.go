package workers_manager

import (
	"strings"

	"github.com/maxogod/distro-tp/src/common/middleware"
	"github.com/maxogod/distro-tp/src/common/models/enum"
)

type workerStatus struct {
	finished   bool
	connection middleware.MessageMiddleware
}

type workersManager struct {
	middlewareUrl string
	rrCounter     int
	rrIDs         []string

	filterWorkers     map[string]workerStatus
	groupByWorkers    map[string]workerStatus
	reducerWorkers    map[string]workerStatus
	joinerWorkers     map[string]workerStatus
	aggregatorWorkers map[string]workerStatus
}

func NewWorkersManager(middlewareUrl string) WorkersManager {
	return &workersManager{
		middlewareUrl:     middlewareUrl,
		rrCounter:         0,
		rrIDs:             []string{},
		filterWorkers:     make(map[string]workerStatus),
		groupByWorkers:    make(map[string]workerStatus),
		reducerWorkers:    make(map[string]workerStatus),
		joinerWorkers:     make(map[string]workerStatus),
		aggregatorWorkers: make(map[string]workerStatus),
	}
}

func (wm *workersManager) AddWorker(id string) error {
	group := strings.Split(id, "_")[0]
	workerStat := workerStatus{
		finished:   false,
		connection: middleware.GetDataExchange(wm.middlewareUrl, []string{id}),
	}
	wm.rrIDs = append(wm.rrIDs, id)
	switch enum.WorkerType(group) {
	case enum.Filter:
		wm.filterWorkers[id] = workerStat
	case enum.GroupBy:
		wm.groupByWorkers[id] = workerStat
	case enum.Reducer:
		wm.reducerWorkers[id] = workerStat
	case enum.Joiner:
		wm.joinerWorkers[id] = workerStat
	case enum.Aggregator:
		wm.aggregatorWorkers[id] = workerStat
	default:
		return &InvalidWorkerTypeError{workerType: group}
	}
	return nil
}

func (wm *workersManager) FinishWorker(id string) error {
	group := strings.Split(id, "_")[0]
	switch enum.WorkerType(group) {
	case enum.Filter:
		workerStat, exists := wm.filterWorkers[id]
		if !exists {
			return &WorkerNotExistsError{}
		}
		workerStat.finished = true
		wm.filterWorkers[id] = workerStat
	case enum.GroupBy:
		workerStat, exists := wm.groupByWorkers[id]
		if !exists {
			return &WorkerNotExistsError{}
		}
		workerStat.finished = true
		wm.groupByWorkers[id] = workerStat
	case enum.Reducer:
		workerStat, exists := wm.reducerWorkers[id]
		if !exists {
			return &WorkerNotExistsError{}
		}
		workerStat.finished = true
		wm.reducerWorkers[id] = workerStat
	case enum.Joiner:
		workerStat, exists := wm.joinerWorkers[id]
		if !exists {
			return &WorkerNotExistsError{}
		}
		workerStat.finished = true
		wm.joinerWorkers[id] = workerStat
	case enum.Aggregator:
		workerStat, exists := wm.aggregatorWorkers[id]
		if !exists {
			return &WorkerNotExistsError{}
		}
		workerStat.finished = true
		wm.aggregatorWorkers[id] = workerStat
	default:
		return &InvalidWorkerTypeError{workerType: group}
	}
	return nil
}

func (wm *workersManager) GetFinishExchangeTopic() (enum.WorkerType, bool) {
	if len(wm.filterWorkers) > 0 {
		for _, workerStat := range wm.filterWorkers {
			if !workerStat.finished {
				return enum.Filter, false
			}
		}
	}
	if len(wm.groupByWorkers) > 0 {
		for _, workerStat := range wm.groupByWorkers {
			if !workerStat.finished {
				return enum.GroupBy, false
			}
		}
	}
	if len(wm.reducerWorkers) > 0 {
		for _, workerStat := range wm.reducerWorkers {
			if !workerStat.finished {
				return enum.Reducer, false
			}
		}
	}
	if len(wm.joinerWorkers) > 0 {
		for _, workerStat := range wm.joinerWorkers {
			if !workerStat.finished {
				return enum.Joiner, false
			}
		}
	}

	return enum.Aggregator, true
}

func (wm *workersManager) GetWorkerConnectionRR() (middleware.MessageMiddleware, error) {
	if len(wm.filterWorkers) == 0 {
		return nil, &WorkerNotExistsError{}
	} else if wm.rrCounter >= len(wm.rrIDs) {
		wm.rrCounter = 0
	}

	id := wm.rrIDs[wm.rrCounter]
	wm.rrCounter++

	workerStat, exists := wm.filterWorkers[id]
	if !exists {
		return nil, &WorkerNotExistsError{}
	}
	return workerStat.connection, nil
}
