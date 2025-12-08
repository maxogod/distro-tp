package gateway_test

import (
	"fmt"
	"testing"
	"time"

	"github.com/maxogod/distro-tp/src/common/logger"
	"github.com/maxogod/distro-tp/src/common/middleware"
	"github.com/maxogod/distro-tp/src/common/models/enum"
	"github.com/maxogod/distro-tp/src/common/models/protocol"
	"github.com/maxogod/distro-tp/src/gateway/config"
	"github.com/maxogod/distro-tp/src/gateway/mock"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
)

const integrationGatewayConfigPath = "./config_test.yaml"

// TestGatewayDropsDuplicateProcessedMessages verifies that the gateway
// message handler only forwards a single copy of duplicate processed envelopes.
func TestGatewayDropsDuplicateProcessedMessages(t *testing.T) {
	logger.InitLogger(logger.LoggerEnvDevelopment)

	clientID := fmt.Sprintf("gateway-dup-it-%d", time.Now().UnixNano())

	conf, err := config.InitConfigWithPath(integrationGatewayConfigPath)
	require.NoError(t, err)

	gateway, err := mock.StartGatewayMock(integrationGatewayConfigPath, clientID)
	require.NoError(t, err)
	defer gateway.Close()

	processedExchange := middleware.GetProcessedDataExchange(conf.MiddlewareAddress, clientID)
	defer func() {
		processedExchange.StopConsuming()
		processedExchange.Delete()
		processedExchange.Close()
	}()

	dataCh := make(chan *protocol.DataEnvelope)
	go gateway.GetReportData(dataCh)

	// Allow the consumer to subscribe before publishing.
	time.Sleep(500 * time.Millisecond)

	payload := []byte("result-payload")
	duplicates := []*protocol.DataEnvelope{
		{
			ClientId:       clientID,
			TaskType:       int32(enum.T1),
			SequenceNumber: 1,
			Payload:        payload,
		},
		{
			ClientId:       clientID,
			TaskType:       int32(enum.T1),
			SequenceNumber: 1,
			Payload:        payload,
		},
	}

	uniqueEnvelope := &protocol.DataEnvelope{
		ClientId:       clientID,
		TaskType:       int32(enum.T1),
		SequenceNumber: 2,
		Payload:        []byte("another-payload"),
	}

	doneEnvelope := &protocol.DataEnvelope{ClientId: clientID, IsDone: true}

	for _, env := range append(duplicates, uniqueEnvelope, doneEnvelope) {
		bytes, err := proto.Marshal(env)
		require.NoError(t, err)
		require.Equal(t, middleware.MessageMiddlewareSuccess, processedExchange.Send(bytes))
	}

	var received []*protocol.DataEnvelope
	timeout := time.After(5 * time.Second)

receiveLoop:
	for {
		select {
		case env, ok := <-dataCh:
			if !ok {
				break receiveLoop
			}
			received = append(received, env)
			if env.GetIsDone() {
				continue
			}
		case <-timeout:
			t.Fatal("timed out waiting for processed data from gateway")
		}
	}

	require.Len(t, received, 4, "expected duplicates not dropped, unique message, and done")
	require.Equal(t, payload, received[0].GetPayload())
	require.EqualValues(t, 1, received[0].GetSequenceNumber())
	require.Equal(t, payload, received[1].GetPayload())
	require.EqualValues(t, 1, received[1].GetSequenceNumber())
	require.Equal(t, []byte("another-payload"), received[2].GetPayload())
	require.EqualValues(t, 2, received[2].GetSequenceNumber())
	require.True(t, received[3].GetIsDone())
}
