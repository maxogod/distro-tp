package worker

import (
	"fmt"

	"github.com/maxogod/distro-tp/src/common/middleware"
	"github.com/maxogod/distro-tp/src/common/models/enum"
	"github.com/maxogod/distro-tp/src/common/utils"
	"google.golang.org/protobuf/proto"
)

func SendDataTo(data proto.Message, taskType enum.TaskType, clientID string, outputQueue middleware.MessageMiddleware) error {

	envelope, err := utils.CreateSerializedEnvelope(data, int32(taskType), clientID)
	if err != nil {
		return fmt.Errorf("failed to envelope message: %v", err)
	}

	if e := outputQueue.Send(envelope); e != middleware.MessageMiddlewareSuccess {
		return fmt.Errorf("failed to send message to output queue: %d", int(e))
	}

	return nil
}
