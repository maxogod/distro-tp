package update_handler

import (
	"github.com/maxogod/distro-tp/src/common/leader_election"
	"github.com/maxogod/distro-tp/src/common/models/protocol"
)

type updateHandler struct {
}

func NewUpdateHandler() leader_election.UpdateCallbacks {
	return &updateHandler{}
}

func (uh *updateHandler) ResetUpdates() {
	// Implementation here
}

func (uh *updateHandler) GetUpdates(receivingCh chan *protocol.DataEnvelope) {
	// Implementation here
}

func (uh *updateHandler) SendUpdates(sendingCh chan *protocol.DataEnvelope, done chan bool) {
	// Implementation here
}
