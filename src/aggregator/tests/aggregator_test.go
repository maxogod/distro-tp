package tests

import (
	"testing"

	aggregator "github.com/maxogod/distro-tp/src/aggregator/business"
	helpers "github.com/maxogod/distro-tp/src/aggregator/tests/test_helpers"
	"github.com/maxogod/distro-tp/src/common/models/data_batch"
	"github.com/maxogod/distro-tp/src/common/models/enum"
	"github.com/maxogod/distro-tp/src/common/models/joined"
	"github.com/stretchr/testify/assert"
	"google.golang.org/protobuf/proto"
)

func TestHandleTaskType4(t *testing.T) {
	storeDir := t.TempDir()

	mostPurchasesUsers := []*joined.JoinMostPurchasesUser{
		{StoreName: "G Coffee @ Seksyen 21", UserBirthdate: "1970-04-22", PurchasesQty: 260611},
		{StoreName: "G Coffee @ Alam Tun Hussein Onn", UserBirthdate: "1974-06-21", PurchasesQty: 91218},
	}
	mostPurchasesUsersBatch := helpers.PrepareJoinMostPurchasesUserBatch(t, mostPurchasesUsers, enum.T4)

	testCase := helpers.CreateTestCaseTask4(storeDir, mostPurchasesUsersBatch, true)

	agg := helpers.StartAggregator(t, storeDir, []string{testCase.Queue})
	defer func(agg *aggregator.Aggregator) {
		err := agg.Stop()
		assert.NoError(t, err)
	}(agg)

	helpers.RunTest(t, testCase)

	received := helpers.GetAllOutputMessages(t, testCase.AggregatorConfig.GatewayControllerDataQueue, func(body []byte) (*data_batch.DataBatch, error) {
		batch := &data_batch.DataBatch{}
		if err := proto.Unmarshal(body, batch); err != nil {
			return nil, err
		}
		return batch, nil
	})

	helpers.AssertAggregatedMostPurchasesUsers(t, received[0], mostPurchasesUsers)

	doneDataMsg := received[1]
	assert.Equal(t, int32(enum.T4), doneDataMsg.TaskType)
	assert.Equal(t, true, doneDataMsg.Done)
}

func TestHandleConnection(t *testing.T) {
	storeDir := t.TempDir()

	aggregatorConfig := helpers.AggregatorConfig(storeDir)

	agg := aggregator.NewAggregator(&aggregatorConfig)

	err := agg.InitService()
	assert.NoError(t, err)

	helpers.AssertConnectionMsg(t, aggregatorConfig.GatewayControllerConnectionQueue, false)
}
