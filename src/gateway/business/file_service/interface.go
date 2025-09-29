package file_service

import (
	"github.com/maxogod/distro-tp/src/common/models/menu_items"
	"github.com/maxogod/distro-tp/src/common/models/store"
	"github.com/maxogod/distro-tp/src/common/models/transaction"
	"github.com/maxogod/distro-tp/src/common/models/transaction_items"
	"github.com/maxogod/distro-tp/src/common/models/user"
)

// FileReader reads the given csv batch by batch and produces to the channel
type FileService interface {
	ReadTransactions(path string, batches_ch chan []*transaction.Transaction)

	ReadTransactionItems(path string) (chan []transaction_items.TransactionItems, error)

	ReadMenuItems(path string) (chan []menu_items.MenuItem, error)

	ReadStores(path string) (chan []store.Store, error)

	ReadUsers(path string) (chan []user.User, error)
}
