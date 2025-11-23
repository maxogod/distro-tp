package leader_election

import (
	"github.com/maxogod/distro-tp/src/common/logger"
	"github.com/maxogod/distro-tp/src/common/models/enum"
	"github.com/maxogod/distro-tp/src/common/models/protocol"
	"google.golang.org/protobuf/proto"
)

func (le *leaderElection) handleCoordinatorMsg(nodeId int32) {
	le.leaderId.Store(nodeId)
	le.beginHeartbeatHandler()
	logger.Logger.Infof("Node %d recognized Node %d as leader", le.id, nodeId)
}

func (le *leaderElection) becomeLeader() {
	le.leaderId.Store(le.id)
	le.sendCoordinatorMessage()
}

func (le *leaderElection) sendCoordinatorMessage() {
	coordMsg := &protocol.SyncMessage{
		NodeId: int32(le.id),
		Action: int32(enum.COORDINATOR),
	}
	payload, err := proto.Marshal(coordMsg)
	if err != nil {
		logger.Logger.Errorf("Failed to marshal COORDINATOR message: %v", err)
		return
	}

	le.coordMiddleware.Send(payload) // Broadcast to all nodes
}
