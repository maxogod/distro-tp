package update_handler

import (
	"context"
	"sync"

	"github.com/maxogod/distro-tp/src/aggregator/internal/task_executor"
	"github.com/maxogod/distro-tp/src/common/leader_election"
	"github.com/maxogod/distro-tp/src/common/logger"
	"github.com/maxogod/distro-tp/src/common/models/data_transfer"
	"github.com/maxogod/distro-tp/src/common/models/enum"
	"github.com/maxogod/distro-tp/src/common/models/protocol"
	"github.com/maxogod/distro-tp/src/common/worker/storage"
	"google.golang.org/protobuf/proto"
)

type updateHandler struct {
	finishedClients *sync.Map // map[string]enum.TaskType
	cxt             context.Context
	cancel          context.CancelFunc
	storageService  storage.StorageService
	finishExecutor  task_executor.FinishExecutor
}

func NewUpdateHandler(finishedClients *sync.Map, finishExecutor task_executor.FinishExecutor) leader_election.UpdateCallbacks {
	uh := &updateHandler{
		finishedClients: finishedClients,
		storageService:  storage.NewDiskMemoryStorage(),
		finishExecutor:  finishExecutor,
	}
	uh.cxt, uh.cancel = context.WithCancel(context.Background())
	return uh
}

func (uh *updateHandler) ResetUpdates() {
	logger.Logger.Info("Oops! Lost leadership, resetting update handler state.")
	// This doesn't need to do anything specific for now.
}

func (uh *updateHandler) GetUpdates(receivingCh chan *protocol.DataEnvelope) {

	for dataEnvelope := range receivingCh {
		select {
		case <-uh.cxt.Done():
			logger.Logger.Info("Update handler context cancelled, stopping GetUpdates.")
			return
		default:
		}

		// since the leader election uses GetIsDone to stop sending data
		// we use GetIsRef to signal that this is finished client data
		if dataEnvelope.GetIsRef() {
			err := uh.storeFinishedClients(dataEnvelope)
			if err != nil {
				logger.Logger.Errorf("Error storing finished clients during update: %v", err)
			}
			logger.Logger.Info("Aggregator updates received successfully!")
			return
		}

		clientID := dataEnvelope.GetClientId()
		storage.StoreBatch(uh.storageService, clientID, []*protocol.DataEnvelope{dataEnvelope})
	}
	logger.Logger.Info("Aggregator updates received successfully!")
}

func (uh *updateHandler) SendUpdates(sendingCh chan *protocol.DataEnvelope, done chan bool) {
	// first we send all of the stored data to the new leader
	allFiles := uh.storageService.GetAllFilesReferences()
	for _, clientID := range allFiles {
		readCh, err := uh.storageService.ReadData(clientID)
		if err != nil {
			logger.Logger.Errorf("Error reading data for client %s during update: %v", clientID, err)
			continue
		}
		for protoBytes := range readCh {

			envelope := &protocol.DataEnvelope{}
			err = proto.Unmarshal(protoBytes, envelope)
			if err != nil {
				logger.Logger.Errorf("Error unmarshalling DataEnvelope for client %s during update: %v", clientID, err)
				continue
			}
			select {
			case <-uh.cxt.Done():
				logger.Logger.Info("Update handler context cancelled, stopping SendUpdates.")
				return
			case sendingCh <- envelope:

			}
		}
	}
	logger.Logger.Info("All stored data sent successfully!")

	// send finished clients info
	finishedClientsEnvelope, err := uh.envelopeFinishedClients()
	if err != nil {
		logger.Logger.Errorf("Error marshalling finished clients during update: %v", err)
		return
	}
	select {
	case <-uh.cxt.Done():
		logger.Logger.Info("Update handler context cancelled before sending finished clients, stopping SendUpdates.")
		return
	case sendingCh <- finishedClientsEnvelope:
		logger.Logger.Info("Finished clients info sent successfully!")
	}

	// signal that all stored data has been sent
	select {
	case <-uh.cxt.Done():
		logger.Logger.Info("Update handler context cancelled before sending done signal, stopping SendUpdates.")
		return
	case sendingCh <- &protocol.DataEnvelope{
		IsDone: true,
	}:
	}
	logger.Logger.Info("Aggregator updates sent successfully!")
	done <- true
	// finish remaining clients
	uh.finishRemainingClients()
}

func (uh *updateHandler) Close() {
	uh.cancel()
}

// ------------ Helper methods ------------

func (uh *updateHandler) storeFinishedClients(dataEnvelope *protocol.DataEnvelope) error {
	finishedClientsList := &data_transfer.AggregatorData{}
	if err := proto.Unmarshal(dataEnvelope.GetPayload(), finishedClientsList); err != nil {
		logger.Logger.Errorf("Error unmarshalling finished clients list: %v", err)
		return err
	}
	for _, clientID := range finishedClientsList.GetFinishingClients() {
		uh.finishedClients.Store(clientID, true)
		logger.Logger.Infof("Client %s marked as finished from leader update.", clientID)
	}
	return nil
}

func (uh *updateHandler) envelopeFinishedClients() (*protocol.DataEnvelope, error) {
	var finishedClientsList []string
	uh.finishedClients.Range(func(key, value any) bool {
		clientID := key.(string)
		finishedClientsList = append(finishedClientsList, clientID)
		return true
	})
	finishedClientsData := &data_transfer.AggregatorData{
		FinishingClients: finishedClientsList,
	}
	payload, err := proto.Marshal(finishedClientsData)
	if err != nil {
		return nil, err
	}
	return &protocol.DataEnvelope{
		Payload: payload,
		IsRef:   true,
	}, nil
}

func (uh *updateHandler) finishRemainingClients() {

	uh.finishedClients.Range(func(key, value any) bool {
		clientID := key.(string)
		taskType := value.(enum.TaskType)
		err := uh.finishExecutor.SendAllData(clientID, taskType)
		if err != nil {
			logger.Logger.Errorf("Error finishing client %s during leadership update: %v", clientID, err)
		} else {
			logger.Logger.Infof("Client %s finished successfully during leadership update.", clientID)
		}
		return true
	})

}
