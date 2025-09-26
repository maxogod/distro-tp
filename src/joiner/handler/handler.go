package handler

import (
	"github.com/maxogod/distro-tp/src/joiner/protocol"
)

type TaskHandler struct {
	taskHandlers map[int32]func(*protocol.DataBatch)
}

func NewTaskHandler() *TaskHandler {
	th := &TaskHandler{}

	th.taskHandlers = map[int32]func(*protocol.DataBatch){
		2: th.handleTaskType2,
		3: th.handleTaskType3,
		4: th.handleTaskType4,
	}

	return th
}

func (th *TaskHandler) HandleTask(taskType int32) func(*protocol.DataBatch) {
	return th.taskHandlers[taskType]
}

func (th *TaskHandler) handleTaskType2(dataBatch *protocol.DataBatch) {

}

func (th *TaskHandler) handleTaskType3(dataBatch *protocol.DataBatch) {

}

func (th *TaskHandler) handleTaskType4(dataBatch *protocol.DataBatch) {

}
