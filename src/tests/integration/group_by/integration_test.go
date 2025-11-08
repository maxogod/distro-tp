package group_by_test

import (
	"testing"

	"github.com/maxogod/distro-tp/src/common/logger"
	"github.com/maxogod/distro-tp/src/common/middleware"
	"github.com/maxogod/distro-tp/src/common/models/enum"
	"github.com/maxogod/distro-tp/src/common/models/group_by"
	"github.com/maxogod/distro-tp/src/common/models/protocol"
	"github.com/maxogod/distro-tp/src/common/utils"
	"github.com/maxogod/distro-tp/src/group_by/mock"
	"github.com/stretchr/testify/assert"
	"google.golang.org/protobuf/proto"
)

var url = "amqp://guest:guest@localhost:5672/"

func TestMain(m *testing.M) {
	go mock.StartGroupByMock("./config_test.yaml")
	logger.InitLogger(logger.LoggerEnvDevelopment)
	m.Run()
}

// TestSequentialRun runs tests in sequence to
// avoid consuming conflicts on the same queues.
func TestSequentialRun(t *testing.T) {
	tests := []func(t *testing.T){
		groupByTask2,
		groupByTask3,
		groupByTask4,
	}

	// Run each test one by one
	for _, test := range tests {
		test(t)
	}

	groupbyOutputQueue := middleware.GetGroupByQueue(url)
	reducerOutputQueue := middleware.GetReducerQueue(url)
	groupbyOutputQueue.Delete()
	reducerOutputQueue.Delete()
}

func groupByTask2(t *testing.T) {
	t.Log("Starting Group By Task 2 test...")
	groupByInputQueue := middleware.GetGroupByQueue(url)
	reducerOutputQueue := middleware.GetReducerQueue(url)

	serializedTransactions, _ := proto.Marshal(&MockTransactionsItemsBatch)

	dataEnvelope := protocol.DataEnvelope{
		ClientId: "test-client-2",
		TaskType: int32(enum.T2),
		Payload:  serializedTransactions,
	}

	serializedDataEnvelope, _ := proto.Marshal(&dataEnvelope)

	groupByInputQueue.Send(serializedDataEnvelope)

	done := make(chan bool, 1)

	var T2_1_counter = 1
	var T2_2_counter = 1

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
				T2_1_counter--
			} else {
				T2_2_counter--
			}

			groupData := &group_by.GroupTransactionItemsBatch{}
			err := proto.Unmarshal(dataBatch.Payload, groupData)

			for _, groupDataItem := range groupData.GroupTransactionItems {
				assert.Nil(t, err)

				key := groupDataItem.ItemId + "@" + groupDataItem.YearMonth

				t.Logf("Key: %s", key)
				expectedGroupData, exists := MockItemsOutputT2[key]
				assert.True(t, exists)
				assert.Equal(t, len(expectedGroupData), len(groupDataItem.TransactionItems))
			}

			if T2_1_counter == 0 && T2_2_counter == 0 {
				break
			}
		}
		done <- true
	})
	<-done
	assert.Equal(t, 0, int(e))

	reducerOutputQueue.StopConsuming()
	groupByInputQueue.Close()
	reducerOutputQueue.Close()
}

func groupByTask3(t *testing.T) {
	t.Log("Starting Group By Task 3 test...")
	groupByInputQueue := middleware.GetGroupByQueue(url)
	reducerOutputQueue := middleware.GetReducerQueue(url)

	serializedTransactions, _ := proto.Marshal(&MockTransactionsBatch)

	dataEnvelope := protocol.DataEnvelope{
		ClientId: "test-client-3",
		TaskType: int32(enum.T3),
		Payload:  serializedTransactions,
	}

	serializedDataEnvelope, _ := proto.Marshal(&dataEnvelope)

	groupByInputQueue.Send(serializedDataEnvelope)

	done := make(chan bool, 1)

	groups := make(map[string]struct{})

	e := reducerOutputQueue.StartConsuming(func(consumeChannel middleware.ConsumeChannel, d chan error) {
		for msg := range consumeChannel {
			msg.Ack(false)
			dataBatch, _ := utils.GetDataEnvelope(msg.Body)
			assert.True(t, enum.TaskType(dataBatch.TaskType) == enum.T3)

			groupData := &group_by.GroupTransactionsBatch{}
			err := proto.Unmarshal(dataBatch.Payload, groupData)

			assert.Nil(t, err)

			for _, groupTransactions := range groupData.GroupedTransactions {
				key := groupTransactions.StoreId + "@" + groupTransactions.Semester

				_, exists := groups[key]
				if !exists {
					groups[key] = struct{}{}
				}

				t.Logf("Key: %s", key)
				expectedGroupData, exists := MockTransactionsOutputT3[key]
				assert.True(t, exists)
				assert.Equal(t, len(expectedGroupData), len(groupTransactions.Transactions))
			}

			if len(groups) == 4 {
				break
			}
		}
		done <- true
	})
	<-done
	assert.Equal(t, 0, int(e))

	reducerOutputQueue.StopConsuming()
	groupByInputQueue.Close()
	reducerOutputQueue.Close()
}

func groupByTask4(t *testing.T) {
	t.Log("Starting Group By Task 4 test...")
	groupByInputQueue := middleware.GetGroupByQueue(url)
	reducerOutputQueue := middleware.GetReducerQueue(url)

	serializedTransactions, _ := proto.Marshal(&MockTransactionsBatch)

	dataEnvelope := protocol.DataEnvelope{
		ClientId: "test-client-4",
		TaskType: int32(enum.T4),
		Payload:  serializedTransactions,
	}

	serializedDataEnvelope, _ := proto.Marshal(&dataEnvelope)

	groupByInputQueue.Send(serializedDataEnvelope)

	done := make(chan bool, 1)

	groups := make(map[string]struct{})

	e := reducerOutputQueue.StartConsuming(func(consumeChannel middleware.ConsumeChannel, d chan error) {
		for msg := range consumeChannel {
			msg.Ack(false)
			dataBatch, _ := utils.GetDataEnvelope(msg.Body)
			assert.True(t, enum.TaskType(dataBatch.TaskType) == enum.T4)

			groupData := &group_by.GroupTransactionsBatch{}
			err := proto.Unmarshal(dataBatch.Payload, groupData)

			assert.Nil(t, err)

			for _, groupTransactions := range groupData.GroupedTransactions {
				key := groupTransactions.StoreId + "@" + groupTransactions.UserId

				_, exists := groups[key]
				if !exists {
					groups[key] = struct{}{}
				}

				t.Logf("Key: %s", key)
				expectedGroupData, exists := MockTransactionsOutputT4[key]
				assert.True(t, exists)
				assert.Equal(t, len(expectedGroupData), len(groupTransactions.Transactions))
			}

			if len(groups) == 4 {
				break
			}
		}
		done <- true
	})
	<-done
	assert.Equal(t, 0, int(e))

	reducerOutputQueue.StopConsuming()
	groupByInputQueue.Close()
	reducerOutputQueue.Close()
}
