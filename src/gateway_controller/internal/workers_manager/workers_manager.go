package workers_manager

import (
	"strings"

	"github.com/maxogod/distro-tp/src/common/logger"
	"github.com/maxogod/distro-tp/src/common/models/enum"
)

var log = logger.GetLogger()

type workersManager struct {
	middlewareUrl string
	rrCounter     int
	rrIDs         []string

	filterWorkers     map[string]bool
	groupByWorkers    map[string]bool
	reducerWorkers    map[string]bool
	joinerWorkers     map[string]bool
	aggregatorWorkers map[string]bool
}

func NewWorkersManager(middlewareUrl string) WorkersManager {
	return &workersManager{
		middlewareUrl:     middlewareUrl,
		rrCounter:         0,
		rrIDs:             []string{},
		filterWorkers:     make(map[string]bool),
		groupByWorkers:    make(map[string]bool),
		reducerWorkers:    make(map[string]bool),
		joinerWorkers:     make(map[string]bool),
		aggregatorWorkers: make(map[string]bool),
	}
}

func (wm *workersManager) AddWorker(id string) error {
	group := strings.Split(id, "_")[0]
	wm.rrIDs = append(wm.rrIDs, id)
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
	group := strings.Split(id, "_")[0]
	switch enum.WorkerType(group) {
	case enum.Filter:
		_, exists := wm.filterWorkers[id]
		if !exists {
			return &WorkerNotExistsError{}
		}
		wm.filterWorkers[id] = true
	case enum.GroupBy:
		_, exists := wm.groupByWorkers[id]
		if !exists {
			return &WorkerNotExistsError{}
		}
		wm.groupByWorkers[id] = true
	case enum.Reducer:
		_, exists := wm.reducerWorkers[id]
		if !exists {
			return &WorkerNotExistsError{}
		}
		wm.reducerWorkers[id] = true
	case enum.Joiner:
		_, exists := wm.joinerWorkers[id]
		if !exists {
			return &WorkerNotExistsError{}
		}
		wm.joinerWorkers[id] = true
	case enum.Aggregator:
		_, exists := wm.aggregatorWorkers[id]
		if !exists {
			return &WorkerNotExistsError{}
		}
		wm.aggregatorWorkers[id] = true
	default:
		return &InvalidWorkerTypeError{workerType: group}
	}
	return nil
}

func (wm *workersManager) GetNextWorkerStageToFinish() (enum.WorkerType, bool) {
	if len(wm.filterWorkers) > 0 {
		for _, isFinished := range wm.filterWorkers {
			if !isFinished {
				return enum.Filter, false
			}
		}
		log.Debug("All filter workers finished")
	}
	if len(wm.groupByWorkers) > 0 {
		for _, isFinished := range wm.groupByWorkers {
			if !isFinished {
				return enum.GroupBy, false
			}
		}
		log.Debug("All group by workers finished")
	}
	if len(wm.reducerWorkers) > 0 {
		for _, isFinished := range wm.reducerWorkers {
			if !isFinished {
				return enum.Reducer, false
			}
		}
		log.Debug("All reducer workers finished")
	}
	if len(wm.joinerWorkers) > 0 {
		for _, isFinished := range wm.joinerWorkers {
			if !isFinished {
				return enum.Joiner, false
			}
		}
		log.Debug("All joiner workers finished")
	}
	if len(wm.aggregatorWorkers) > 0 {
		for _, isFinished := range wm.aggregatorWorkers {
			if !isFinished {
				return enum.Aggregator, false
			}
		}
		log.Debug("All aggregator workers finished")
	}

	return enum.Aggregator, true
}

func (wm *workersManager) ClearStatus() {
	for id, _ := range wm.filterWorkers {
		wm.filterWorkers[id] = false
	}
	for id, _ := range wm.groupByWorkers {
		wm.groupByWorkers[id] = false
	}
	for id, _ := range wm.reducerWorkers {
		wm.reducerWorkers[id] = false
	}
	for id, _ := range wm.joinerWorkers {
		wm.joinerWorkers[id] = false
	}
	for id, _ := range wm.aggregatorWorkers {
		wm.aggregatorWorkers[id] = false
	}
}
