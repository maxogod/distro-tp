package leader_election

import (
	"github.com/maxogod/distro-tp/src/common/logger"
	"github.com/maxogod/distro-tp/src/common/middleware"
	"github.com/maxogod/distro-tp/src/common/models/enum"
	"github.com/maxogod/distro-tp/src/common/models/protocol"

	"google.golang.org/protobuf/proto"
)

// startDiscoveryPhase initializes node for discovery phase to request messages from other nodes
// to find out the leader and ask for updates.
func (le *leaderElection) startDiscoveryPhase() chan bool {
	le.readyForElection.Store(false)

	le.sendDiscoveryMessage()

	// This should only timeout at startup
	leaderSearchTimerCh := le.initLeaderSearchTimer(func() {
		// There is no leader -> start election
		le.readyForElection.Store(true)
		le.electionHandler.StartElection()
	})

	return leaderSearchTimerCh
}

// handleDiscoverMsg handles the discovery message whether its the broadcast message on a node connection (marked by leaderID = -1)
// or a response message to that new node and check if the leader is known.
func (le *leaderElection) handleDiscoverMsg(nodeID, leaderID int32, leaderSearchTimerCh *chan bool) {
	if nodeID == le.id {
		return // Ingore self-messages
	}

	if le.leaderId.Load() == 0 && leaderID > 0 && !le.readyForElection.Load() { // there is already a leader
		le.leaderId.Store(leaderID)
		*leaderSearchTimerCh <- true // stop leader search timer
		logger.Logger.Infof("[Node %d] recognized node %d as existing leader", le.id, leaderID)

		go func() { // Dont block main routine on awaiting updates
			err := le.awaitUpdates()
			if err != nil {
				logger.Logger.Errorf("[Node %d] Failed to get updates from leader %d: %v", le.id, leaderID, err)
				// Re-start discovery phase (leader fell mid updating this node)
				*leaderSearchTimerCh = le.startDiscoveryPhase()
			}

			le.readyForElection.Store(true)
			le.beginHeartbeatHandler()
		}()
	} else if leaderID == -1 { // discovery message
		le.respondDiscoveryMessage(nodeID)
	}
}

// sendDiscoveryMessage sends the connection message as a new node in the cluster.
// discovery msg && leaderID = -1 -> new node.
func (le *leaderElection) sendDiscoveryMessage() {
	discoveryMsg := &protocol.SyncMessage{
		NodeId:   int32(le.id),
		Action:   int32(enum.DISCOVER),
		LeaderId: -1, // Indicates discovery
	}

	msgBytes, err := proto.Marshal(discoveryMsg)
	if err != nil {
		logger.Logger.Errorf("[Node %d] Failed to marshal discovery message: %v", le.id, err)
		return
	}

	e := le.connMiddleware.Send(msgBytes)
	if e != middleware.MessageMiddlewareSuccess {
		logger.Logger.Errorf("[Node %d] Failed to send discovery message: %d", le.id, int(e))
	}
}

// respondDiscoveryMessage handles the sending of the response message to the new node, along with the known leaderID (or 0).
func (le *leaderElection) respondDiscoveryMessage(nodeId int32) {
	responseMsg := &protocol.SyncMessage{
		NodeId:   int32(le.id),
		Action:   int32(enum.DISCOVER),
		LeaderId: le.leaderId.Load(),
	}

	msgBytes, err := proto.Marshal(responseMsg)
	if err != nil {
		logger.Logger.Errorf("[Node %d] Failed to marshal discovery response message: %v", le.id, err)
		return
	}

	keys := make([]int32, 0, len(le.connectedNodes))
	for k := range le.connectedNodes {
		keys = append(keys, k)
	}
	logger.Logger.Debugf("Connected node keys: %v", keys)
	m, exists := le.connectedNodes[nodeId]
	if !exists {
		logger.Logger.Errorf("[Node %d] No middleware found for node %d to respond to discovery", le.id, nodeId)
		return
	}

	e := m.Send(msgBytes)
	if e != middleware.MessageMiddlewareSuccess {
		logger.Logger.Errorf("[Node %d] Failed to send discovery response message to node %d: %d", le.id, nodeId, int(e))
	}
}
