package leader_election

import (
	"sync/atomic"
	"time"

	"github.com/maxogod/distro-tp/src/common/logger"
	"github.com/maxogod/distro-tp/src/common/middleware"
	"github.com/maxogod/distro-tp/src/common/models/enum"
	"github.com/maxogod/distro-tp/src/common/models/protocol"
	"google.golang.org/protobuf/proto"
)

/* --- ELECTION ACTION HANDLING FOR LEADER ELECTION --- */

func (le *leaderElection) handleElectionMsg(nodeId int) {
	le.sendAckMessage(nodeId)

	electionMsg := &protocol.SyncMessage{
		NodeId: int32(le.id),
		Action: int32(enum.ELECTION),
	}

	electionBytes, err := proto.Marshal(electionMsg)
	if err != nil {
		logger.Logger.Errorf("Failed to marshal ELECTION message: %v", err)
		return
	}

	foundHigher := false
	higherIDs := make([]int, 0)
	for id := range le.connectedNodes {
		if id > le.id {
			foundHigher = true
			higherIDs = append(higherIDs, id)
		}
	}

	if !foundHigher {
		le.sendCoordinatorMessage()

		le.leaderId = le.id
		logger.Logger.Infof("Node %d became coordinator", le.id)
		return
	}

	for _, id := range higherIDs {
		if conn, ok := le.connectedNodes[id]; ok {
			if e := conn.Send(electionBytes); e != middleware.MessageMiddlewareSuccess {
				logger.Logger.Errorf("Failed to forward ELECTION to node %d: %d", id, int(e))
			}
		}
	}

	myRound := atomic.AddUint64(&le.round, 1)
	go le.runElectionTimeout(myRound, nodeId)
}

func (le *leaderElection) runElectionTimeout(roundID uint64, nodeId int) {
	timer := time.NewTimer(le.ackTimeout)
	defer timer.Stop()
	gotAck := false

	for {
		select {
		case r := <-le.ackCh:
			if r != roundID {
				// outdated ack for previous round; ignore
				continue
			}

			gotAck = true

			if !timer.Stop() {
				select {
				case <-timer.C:
				default:
				}
			}

			timer.Reset(le.coordTimeout)

		case cr := <-le.coordCh:
			if cr != roundID {
				// outdated coordinator for previous round
				continue
			}
			logger.Logger.Debugf("Node %d received COORDINATOR for round %d", le.id, roundID)
			return

		case <-timer.C:
			if gotAck {
				logger.Logger.Infof("Coordinator timeout expired on node %d; restarting election", le.id)
				go le.handleElectionMsg(nodeId)
				return
			} else {
				logger.Logger.Infof("ACK timeout expired on node %d; no higher nodes responded, becoming leader", le.id)
				le.sendCoordinatorMessage()
				le.leaderId = le.id
				logger.Logger.Infof("Node %d became coordinator", le.id)
				return
			}
		}
	}
}

func (le *leaderElection) sendAckMessage(nodeId int) {
	senderMiddleware, _ := le.connectedNodes[nodeId]

	ackMsg := &protocol.SyncMessage{
		NodeId: int32(le.id),
		Action: int32(enum.ACK),
	}

	ackBytes, err := proto.Marshal(ackMsg)
	if err != nil {
		logger.Logger.Errorf("Failed to marshal ACK message: %v", err)
		return
	}

	if e := senderMiddleware.Send(ackBytes); e != middleware.MessageMiddlewareSuccess {
		logger.Logger.Errorf("Failed to send ACK to node %d: %d", nodeId, int(e))
	}
}

func (le *leaderElection) sendCoordinatorMessage() {
	coordMsg := &protocol.SyncMessage{
		NodeId: int32(le.id),
		Action: int32(enum.COORDINATOR),
	}
	coordBytes, errMarshal := proto.Marshal(coordMsg)
	if errMarshal != nil {
		logger.Logger.Errorf("Failed to marshal COORDINATOR message: %v", errMarshal)
		return
	}

	if e := le.coordMiddleware.Send(coordBytes); e != middleware.MessageMiddlewareSuccess {
		logger.Logger.Errorf("Failed to send COORDINATOR message")
	}
}
