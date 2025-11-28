package update_handler

import (
	"sync"

	"github.com/maxogod/distro-tp/src/common/leader_election"
	"github.com/maxogod/distro-tp/src/common/models/protocol"
)

type updateHandler struct {
	finishedClients *sync.Map
}

func NewUpdateHandler(finishedClients *sync.Map) leader_election.UpdateCallbacks {
	return &updateHandler{
		finishedClients: finishedClients,
	}
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
