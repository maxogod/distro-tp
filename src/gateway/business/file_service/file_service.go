package file_service

import (
	"github.com/maxogod/distro-tp/src/common/models/menu_items"
	"github.com/maxogod/distro-tp/src/common/models/store"
	"github.com/maxogod/distro-tp/src/common/models/transaction"
	"github.com/maxogod/distro-tp/src/common/models/transaction_items"
	"github.com/maxogod/distro-tp/src/common/models/user"
)

type fileService struct{}

func NewFileService() FileService {
	return &fileService{}
}

func (fs *fileService) ReadTransactions(path string) (chan []transaction.Transaction, error) {
	return nil, nil
}

func (fs *fileService) ReadTransactionItems(path string) (chan []transaction_items.TransactionItems, error) {
	return nil, nil
}

func (fs *fileService) ReadMenuItems(path string) (chan []menu_items.MenuItem, error) {
	return nil, nil
}

func (fs *fileService) ReadStores(path string) (chan []store.Store, error) {
	return nil, nil
}

func (fs *fileService) ReadUsers(path string) (chan []user.User, error) {
	return nil, nil
}
