package test_helpers

import (
	"testing"

	"github.com/maxogod/distro-tp/src/common/middleware"
	"github.com/maxogod/distro-tp/src/common/models/enum"
	"github.com/stretchr/testify/assert"
)

type TestCase struct {
	Queue         string
	DatasetType   enum.RefDatasetType
	CsvPayloads   [][]byte
	ExpectedFiles []string
	TaskDone      enum.TaskType
	SendDone      bool
}

func RunTest(t *testing.T, c TestCase) {
	t.Helper()

	pub, err := middleware.NewQueueMiddleware(RabbitURL, c.Queue)
	assert.NoError(t, err)
	defer func() {
		_ = pub.Delete()
		_ = pub.Close()
	}()

	SendReferenceBatches(t, pub, c.CsvPayloads, c.DatasetType, c.TaskDone)

	for _, expectedFile := range c.ExpectedFiles {
		switch c.DatasetType {
		case enum.Users:
			AssertUsersAreTheExpected(t, expectedFile, c.CsvPayloads, c.DatasetType, c.TaskDone)
		case enum.Stores:
			AssertStoresAreTheExpected(t, expectedFile, c.CsvPayloads, c.DatasetType, c.TaskDone)
		case enum.MenuItems:
			AssertMenuItemsAreTheExpected(t, expectedFile, c.CsvPayloads, c.DatasetType, c.TaskDone)
		}
	}

	if c.SendDone {
		SendDoneMessage(t, pub, c.TaskDone)
	}
}
