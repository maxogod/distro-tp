package task_executor

import "github.com/maxogod/distro-tp/src/common/models/enum"

type FinishExecutor interface {
	SendAllData(clientID string, taskType enum.TaskType) error
}
