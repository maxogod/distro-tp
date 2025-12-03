package task_executor

import "github.com/maxogod/distro-tp/src/common/models/enum"

type FinishExecutor interface {
	// SendAllData sends all aggregated data for a given client and task type.
	SendAllData(clientID string, taskType enum.TaskType) error
	// AckClientMessages acknowledges all messages for a given client.
	AckClientMessages(clientID string) error
}
