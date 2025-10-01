package workers_manager

import (
	"strings"

	"github.com/maxogod/distro-tp/src/common/models/enum"
)

type workersManager struct {
	filterWorkers     map[string]bool
	groupByWorkers    map[string]bool
	reducerWorkers    map[string]bool
	joinerWorkers     map[string]bool
	aggregatorWorkers map[string]bool
}

func NewWorkersManager() WorkersManager {
	return &workersManager{
		filterWorkers:     make(map[string]bool),
		groupByWorkers:    make(map[string]bool),
		reducerWorkers:    make(map[string]bool),
		joinerWorkers:     make(map[string]bool),
		aggregatorWorkers: make(map[string]bool),
	}
}

func (wm *workersManager) AddWorker(id string) error {
	group := strings.Split(id, "-")[0]
	switch enum.WorkerType(group) {
	case enum.Filter:
		wm.filterWorkers[id] = false
	case enum.GroupBy:
		wm.groupByWorkers[id] = false
	case enum.Reducer:
		wm.reducerWorkers[id] = false
	case enum.Joiner:
		wm.joinerWorkers[id] = false
	case enum.Aggregator:
		wm.aggregatorWorkers[id] = false
	default:
		return &InvalidWorkerTypeError{workerType: group}
	}
	return nil
}

func (wm *workersManager) FinishWorker(id string) error {
	group := strings.Split(id, "-")[0]
	switch enum.WorkerType(group) {
	case enum.Filter:
		if _, exists := wm.filterWorkers[id]; !exists {
			return &WorkerNotExistsError{}
		}
		wm.filterWorkers[id] = true
	case enum.GroupBy:
		if _, exists := wm.groupByWorkers[id]; !exists {
			return &WorkerNotExistsError{}
		}
		wm.groupByWorkers[id] = true
	case enum.Reducer:
		if _, exists := wm.reducerWorkers[id]; !exists {
			return &WorkerNotExistsError{}
		}
		wm.reducerWorkers[id] = true
	case enum.Joiner:
		if _, exists := wm.joinerWorkers[id]; !exists {
			return &WorkerNotExistsError{}
		}
		wm.joinerWorkers[id] = true
	case enum.Aggregator:
		if _, exists := wm.aggregatorWorkers[id]; !exists {
			return &WorkerNotExistsError{}
		}
		wm.aggregatorWorkers[id] = true
	default:
		return &InvalidWorkerTypeError{workerType: group}
	}
	return nil
}

func (wm *workersManager) GetFinishExchangeTopic() (string, bool) {
	if len(wm.filterWorkers) > 0 {
		for _, finished := range wm.filterWorkers {
			if !finished {
				return string(enum.Filter), false
			}
		}
	}
	if len(wm.groupByWorkers) > 0 {
		for _, finished := range wm.groupByWorkers {
			if !finished {
				return string(enum.GroupBy), false
			}
		}
	}
	if len(wm.reducerWorkers) > 0 {
		for _, finished := range wm.reducerWorkers {
			if !finished {
				return string(enum.Reducer), false
			}
		}
	}
	if len(wm.joinerWorkers) > 0 {
		for _, finished := range wm.joinerWorkers {
			if !finished {
				return string(enum.Joiner), false
			}
		}
	}

	return string(enum.Aggregator), true
}
