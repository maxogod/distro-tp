package leader_election

import (
	"github.com/maxogod/distro-tp/src/common/logger"
	"github.com/maxogod/distro-tp/src/common/middleware"
	"github.com/maxogod/distro-tp/src/common/models/enum"
	"github.com/maxogod/distro-tp/src/common/models/protocol"

	"google.golang.org/protobuf/proto"
)

// handleDiscoverMsg handles the discovery message whether its the broadcast message on a node connection (marked by leaderID = -1)
// or a response message to that new node and check if the leader is known.
func (le *leaderElection) handleDiscoverMsg(nodeID, leaderID int32, leaderSearchTimerCh chan bool) {
	if leaderID > 0 && !le.readyForElection.Load() { // there is already a leader
		le.leaderId.Store(leaderID)
		leaderSearchTimerCh <- true // stop leader search timer

		le.awaitUpdates() // TODO: if timeout, redo the discovery phase

		le.readyForElection.Store(true)
		le.beginHeartbeatHandler()
		logger.Logger.Infof("Node %d recognized node %d as coordinator", le.id, leaderID)
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
		logger.Logger.Errorf("Failed to marshal discovery message: %v", err)
		return
	}

	e := le.connMiddleware.Send(msgBytes)
	if e != middleware.MessageMiddlewareSuccess {
		logger.Logger.Errorf("Failed to send discovery message: %d", int(e))
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
		logger.Logger.Errorf("Failed to marshal discovery response message: %v", err)
		return
	}

	m, exists := le.connectedNodes[nodeId]
	if !exists {
		logger.Logger.Errorf("No middleware found for node %d to respond to discovery", nodeId)
		return
	}

	e := m.Send(msgBytes)
	if e != middleware.MessageMiddlewareSuccess {
		logger.Logger.Errorf("Failed to send discovery response message to node %d: %d", nodeId, int(e))
	}
}
