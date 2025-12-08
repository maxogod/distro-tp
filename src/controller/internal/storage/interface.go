package storage

import (
	"github.com/maxogod/distro-tp/src/common/models/enum"
	"github.com/maxogod/distro-tp/src/common/models/protocol"
)

type CounterStorage interface {
	// GetClientIds returns the list of client IDs stored in the storage, based on the files in the storage directory
	GetClientIds() ([]string, error)

	// ReadClientCounters returns the list of counters for the given client
	ReadClientCounters(clientID string) ([]*protocol.MessageCounter, error)

	// AppendCounters appends the given counters to the counter file for the given client
	AppendCounters(clientID string, counters []*protocol.MessageCounter, taskType enum.TaskType) error

	// RemoveClient removes the counter file for the given client
	RemoveClient(clientID string) error
}
