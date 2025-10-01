package test_helpers

import "C"
import (
	"fmt"
	"path/filepath"
	"testing"

	"github.com/maxogod/distro-tp/src/aggregator/config"
	"github.com/maxogod/distro-tp/src/common/middleware"
	"github.com/maxogod/distro-tp/src/common/models/data_batch"
	"github.com/maxogod/distro-tp/src/common/models/enum"
	"github.com/stretchr/testify/assert"
)

const (
	RabbitURL = "amqp://guest:guest@localhost:5672/"
)

type TestCase struct {
	Queue            string
	DataBatch        *data_batch.DataBatch
	ExpectedFile     string
	TaskDone         enum.TaskType
	SendDone         bool
	AggregatorConfig config.Config
	IsBestSelling    bool
}

func CreateTestCaseTask2(
	storeDir string,
	dataBach *data_batch.DataBatch,
	expectedFileName string,
	isBestSelling bool,
	sendDone bool,
) TestCase {
	return TestCase{
		Queue: func() string {
			if isBestSelling {
				return "joined_best_selling_transactions"
			} else {
				return "joined_most_profits_transactions"
			}
		}(),
		DataBatch:        dataBach,
		ExpectedFile:     filepath.Join(storeDir, fmt.Sprintf("%s.pb", expectedFileName)),
		TaskDone:         enum.T2,
		SendDone:         sendDone,
		AggregatorConfig: AggregatorConfig(storeDir),
		IsBestSelling:    isBestSelling,
	}
}

func CreateTestCaseTask3(storeDir string, dataBach *data_batch.DataBatch, sendDone bool) TestCase {
	return TestCase{
		Queue:            "joined_stores_tpv",
		DataBatch:        dataBach,
		ExpectedFile:     filepath.Join(storeDir, "task3.pb"),
		TaskDone:         enum.T3,
		SendDone:         sendDone,
		AggregatorConfig: AggregatorConfig(storeDir),
		IsBestSelling:    false,
	}
}

func CreateTestCaseTask4(storeDir string, dataBach *data_batch.DataBatch, sendDone bool) TestCase {
	return TestCase{
		Queue:            "joined_user_transactions",
		DataBatch:        dataBach,
		ExpectedFile:     filepath.Join(storeDir, "task4.pb"),
		TaskDone:         enum.T4,
		SendDone:         sendDone,
		AggregatorConfig: AggregatorConfig(storeDir),
		IsBestSelling:    false,
	}
}

func RunTest(t *testing.T, c TestCase) {
	t.Helper()

	pub, err := middleware.NewQueueMiddleware(RabbitURL, c.Queue)
	assert.NoError(t, err)
	defer func() {
		_ = pub.Delete()
		_ = pub.Close()
	}()

	SendDataBatch(t, pub, c.DataBatch)

	switch c.TaskDone {
	case enum.T2:
		if c.IsBestSelling {
			AssertPersistentBestSellingProducts(t, c.ExpectedFile, c.DataBatch)
		} else {
			AssertPersistentMostProfitsProducts(t, c.ExpectedFile, c.DataBatch)
		}
	case enum.T3:
		AssertPersistentJoinedStoresTPV(t, c.ExpectedFile, c.DataBatch)
	case enum.T4:
		AssertPersistentJoinedUserTransactions(t, c.ExpectedFile, c.DataBatch)
	default:
		panic("unhandled default case")
	}

	if c.SendDone {
		SendDoneMessage(t, c.AggregatorConfig, c.TaskDone)
	}
}
