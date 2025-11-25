package manager

import (
	"github.com/maxogod/distro-tp/src/common/models/enum"
	"github.com/maxogod/distro-tp/src/controller/internal/sessions/clients"
)

// ClientManager defines the interface for managing client sessions.
type ClientManager interface {
	// AddClient creates and registers a new client session.
	AddClient(id string, taskType enum.TaskType) clients.ClientSession

	// RemoveClient removes a client session by its ID.
	RemoveClient(id string)

	// ReapStaleClients removes clients that are finished and have not been closed.
	ReapStaleClients()

	// Close gracefully shuts down the client manager and all its sessions.
	Close()
}
