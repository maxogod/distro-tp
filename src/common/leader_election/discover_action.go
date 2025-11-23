package leader_election

import (
	"github.com/maxogod/distro-tp/src/common/logger"
	"github.com/maxogod/distro-tp/src/common/middleware"
	"github.com/maxogod/distro-tp/src/common/models/enum"
	"github.com/maxogod/distro-tp/src/common/models/protocol"

	"google.golang.org/protobuf/proto"
)

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
