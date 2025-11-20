package leader_election

import (
	"fmt"
	"strconv"

	"github.com/maxogod/distro-tp/src/common/logger"
	"github.com/maxogod/distro-tp/src/common/middleware"
	"github.com/maxogod/distro-tp/src/common/models/enum"
	"github.com/maxogod/distro-tp/src/common/models/protocol"
	"github.com/maxogod/distro-tp/src/common/utils"
	"google.golang.org/protobuf/proto"
)

type leader_election struct {
	id              int
	leaderId        int
	url             string
	workerType      enum.WorkerType
	coordMiddleware middleware.MessageMiddleware
	connMiddleware  middleware.MessageMiddleware
	updateChan      chan protocol.DataEnvelope
	connectedNodes  map[int]middleware.MessageMiddleware
}

func NewLeaderElection(
	id int,
	middlewareUrl string,
	workerType enum.WorkerType,
) LeaderElection {
	le := &leader_election{
		id:              id,
		url:             middlewareUrl,
		workerType:      workerType,
		coordMiddleware: middleware.GetLeaderElectionExchange(middlewareUrl, workerType, "coord"),
		connMiddleware:  middleware.GetLeaderElectionExchange(middlewareUrl, workerType, "conn"),

		updateChan:     make(chan protocol.DataEnvelope),
		connectedNodes: make(map[int]middleware.MessageMiddleware),
	}
	return le
}

func (le *leader_election) IsLeader() bool {
	return le.leaderId == le.id
}

func (le *leader_election) Close() error {
	close(le.updateChan)
	return nil
}

func (le *leader_election) FinishClient(clientID string) error {
	if !le.IsLeader() {
		return nil
	}
	// Notify other nodes about client finish
	for _, connMiddleware := range le.connectedNodes {
		msg := protocol.DataEnvelope{
			IsDone:   true,
			ClientId: clientID,
		}

		envelopeBytes, err := proto.Marshal(&msg)
		if err != nil {
			return err
		}

		e := connMiddleware.Send(envelopeBytes)
		if e != middleware.MessageMiddlewareSuccess {
			return fmt.Errorf("failed to send finish message for client %s", clientID)
		}
	}
	return nil
}

func (le *leader_election) Start(
	resetUpdates func(),
	get_updates chan *protocol.DataEnvelope,
	send_updates func(chan *protocol.DataEnvelope),
) error {
	listenConnectionsChan := make(chan bool)

	// reset any previous updates
	resetUpdates()

	// send that im alive to the world

	// get all node connections
	go le.getAllConnectedNodes(listenConnectionsChan)

	// begin recv data loop / heartbeat monitoring / election handleing

	return nil
}

/* --- Private Methods --- */

func (le *leader_election) getAllConnectedNodes(listenConnectionsChan chan bool) error {

	currentNodeExchange := middleware.GetLeaderElectionExchange(le.url, le.workerType, strconv.Itoa(le.id))
	for {
		select {
		case <-listenConnectionsChan:
			return nil
		default:
			// input channel del mismo nodo
			e := currentNodeExchange.StartConsuming(func(consumeChannel middleware.ConsumeChannel, d chan error) {
				for msg := range consumeChannel {

					dataBatch, err := utils.GetDataEnvelope(msg.Body)
					if err != nil {
						logger.Logger.Errorf("Failed to unmarshal message: %v", err)
						return
					}
					syncMessage := &protocol.SyncMessage{}
					err = proto.Unmarshal(dataBatch.GetPayload(), syncMessage)
					if err != nil {
						logger.Logger.Errorf("Failed to unmarshal sync message: %v", err)
						return
					}
					nodeID := int(syncMessage.GetNodeId())
					connectedNode := middleware.GetLeaderElectionExchange(le.url, le.workerType, strconv.Itoa(nodeID))

					le.connectedNodes[nodeID] = connectedNode
				}
			})
			if e != middleware.MessageMiddlewareSuccess {
				return fmt.Errorf("an error occurred while starting consumption: %d", int(e))
			}
			return nil
		}
	}
}
