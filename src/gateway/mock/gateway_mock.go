package mock

import (
	"github.com/maxogod/distro-tp/src/common/logger"
	"github.com/maxogod/distro-tp/src/common/models/protocol"
	"github.com/maxogod/distro-tp/src/gateway/config"
	"github.com/maxogod/distro-tp/src/gateway/internal/handler"
)

type MockGateway struct {
	handler handler.MessageHandler
}

func StartGatewayMock(configPath, clientID string) (*MockGateway, error) {
	conf, err := config.InitConfigWithPath(configPath)
	if err != nil {
		return nil, err
	}

	if conf.MaxControllerNodes == 0 {
		conf.MaxControllerNodes = 1
	}

	logger.InitLogger(logger.LoggerEnvDevelopment)

	return &MockGateway{
		handler: handler.NewMessageHandler(conf.MiddlewareAddress, clientID, conf),
	}, nil
}

func (m *MockGateway) GetReportData(ch chan *protocol.DataEnvelope) {
	m.handler.GetReportData(ch)
}

func (m *MockGateway) Close() {
	m.handler.Close()
}