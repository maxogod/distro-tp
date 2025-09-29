package test_helpers

import (
	"testing"

	"github.com/maxogod/distro-tp/src/common/middleware"
	"github.com/maxogod/distro-tp/src/common/models"
	"github.com/stretchr/testify/assert"
)

type TestCase struct {
	Queue         string
	DatasetType   models.RefDatasetType
	CsvPayloads   [][]byte
	ExpectedFiles []string
	TaskDone      models.TaskType
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

	SendReferenceBatches(t, pub, c.CsvPayloads, c.DatasetType)

	for _, expectedFile := range c.ExpectedFiles {
		switch c.DatasetType {
		case models.Users:
			AssertUsersAreTheExpected(t, expectedFile, c.CsvPayloads, c.DatasetType)
		case models.Stores:
			AssertStoresAreTheExpected(t, expectedFile, c.CsvPayloads, c.DatasetType)
		case models.MenuItems:
			AssertMenuItemsAreTheExpected(t, expectedFile, c.CsvPayloads, c.DatasetType)
		}
	}

	if c.SendDone {
		SendDoneMessage(t, pub, c.TaskDone)
	}
}
