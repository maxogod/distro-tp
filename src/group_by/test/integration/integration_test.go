package integration_test

import (
	"testing"

	"github.com/maxogod/distro-tp/src/common/middleware"
	"github.com/maxogod/distro-tp/src/common/models/enum"
	"github.com/maxogod/distro-tp/src/common/models/group_by"
	"github.com/maxogod/distro-tp/src/common/models/protocol"
	"github.com/maxogod/distro-tp/src/common/utils"
	"github.com/maxogod/distro-tp/src/group_by/config"
	"github.com/maxogod/distro-tp/src/group_by/internal/server"
	"github.com/maxogod/distro-tp/src/group_by/test/integration"
	"github.com/stretchr/testify/assert"
	"google.golang.org/protobuf/proto"
)

func startGroupByMock() {
	conf, err := config.InitConfig("./config_test.yaml")
	if err != nil {
		panic(err)
	}
	server := server.InitServer(conf)
	server.Run()
}

func TestGroupByTask2(t *testing.T) {

	url := "amqp://guest:guest@localhost:5672/"

	groupByInputQueue := middleware.GetGroupByQueue(url)
	reducerOutputQueue := middleware.GetReducerQueue(url)

	defer groupByInputQueue.Close()
	defer reducerOutputQueue.Close()

	serializedTransactions, _ := proto.Marshal(&integration.MockTransactionsItemsBatch)

	dataEnvelope := protocol.DataEnvelope{
		ClientId: "test-client",
		TaskType: int32(enum.T2),
		Payload:  serializedTransactions,
	}

	serializedDataEnvelope, _ := proto.Marshal(&dataEnvelope)

	groupByInputQueue.Send(serializedDataEnvelope)

	go func() {
		startGroupByMock()
	}()

	done := make(chan bool, 1)

	var T2_1_counter = 0
	var T2_2_counter = 0

	// I expect the group by worker to send 6 batches, 2 for each group
	// since the worker sends them to 2 different task types (T2_1 and T2_2)
	// In total, there should be these unique groups (itemID@YearMonth):
	// item1@2025-07, item1@2025-08, item3@2025-07
	// so i expect to receive 6 messages in total
	// each message should contain the grouped items
	e := reducerOutputQueue.StartConsuming(func(consumeChannel middleware.ConsumeChannel, d chan error) {
		for msg := range consumeChannel {
			msg.Ack(false)
			dataBatch, _ := utils.GetDataEnvelope(msg.Body)
			assert.True(t, enum.TaskType(dataBatch.TaskType) == enum.T2_1 || enum.TaskType(dataBatch.TaskType) == enum.T2_2)
			if enum.TaskType(dataBatch.TaskType) == enum.T2_1 {
				T2_1_counter++
			} else {
				T2_2_counter++
			}

			groupData := &group_by.GroupTransactionItems{}
			err := proto.Unmarshal(dataBatch.Payload, groupData)

			assert.Nil(t, err)

			key := groupData.ItemId + "@" + groupData.YearMonth

			t.Logf("Key: %s", key)
			expectedGroupData, exists := integration.MockItemsOutputT2[key]
			assert.True(t, exists)
			assert.Equal(t, len(expectedGroupData), len(groupData.TransactionItems))

			if T2_1_counter+T2_2_counter == 6 {
				break
			}
		}
		done <- true
	})
	<-done
	assert.Equal(t, 0, int(e))
}
