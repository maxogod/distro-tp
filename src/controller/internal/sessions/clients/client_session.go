package clients

import (
	"sync/atomic"

	"github.com/maxogod/distro-tp/src/common/logger"
	"github.com/maxogod/distro-tp/src/common/models/enum"
	"github.com/maxogod/distro-tp/src/controller/internal/handler"
)

type clientSession struct {
	Id             string
	controlHandler handler.ControlHandler
	running        atomic.Bool
}

func NewClientSession(id string, controlHandler handler.ControlHandler) ClientSession {
	s := &clientSession{
		Id:             id,
		controlHandler: controlHandler,
	}
	s.running.Store(true)
	return s
}

func (cs *clientSession) IsFinished() bool {
	return !cs.running.Load()
}

func (cs *clientSession) InitiateControlSequence() error {
	logger.Logger.Debugf("[%s] Starting EOF control sequence", cs.Id)

	err := cs.controlHandler.AwaitForWorkers()
	if err != nil {
		logger.Logger.Errorf("[%s] Error awaiting for workers to finish processing data for client: %v", cs.Id, err)
		return err
	}

	logger.Logger.Debugf("[%s] EOF control finished, sending done signal to workers", cs.Id)

	workersToNotify := []enum.WorkerType{
		enum.FilterWorker, enum.GroupbyWorker,
		enum.ReducerWorker, enum.JoinerWorker,
		enum.AggregatorWorker,
	}
	for _, worker := range workersToNotify {
		if err = cs.controlHandler.SendDone(worker, 0, true); err != nil {
			logger.Logger.Errorf("[%s] Error sending done signal to %s for client: %v", cs.Id, string(worker), err)
			return err
		}
	}
	cs.controlHandler.CleanupStorage()
	cs.Close()
	logger.Logger.Debugf("[%s] EOF delivered, and session closed", cs.Id)

	return nil
}

func (cs *clientSession) SendControllerReady() {
	cs.controlHandler.SendControllerReady()
}

func (cs *clientSession) Close() {
	if !cs.IsFinished() {
		cs.controlHandler.CleanupStorage()
		cs.controlHandler.Close()
		cs.running.Store(false)
		logger.Logger.Debugf("[%s] Closed client session", cs.Id)
	}
}
