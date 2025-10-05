package worker

import (
	"github.com/maxogod/distro-tp/src/common/middleware"
	"github.com/maxogod/distro-tp/src/common/models/enum"
	"google.golang.org/protobuf/proto"
)

type MessageHandler interface {
	Start() error
	SendDataToMiddleware(data proto.Message, taskType enum.TaskType, clientID string, outputQueue middleware.MessageMiddleware) error
	FinishClient(clientID string) error
	Close() error
}
