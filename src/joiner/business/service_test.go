package business_test

import (
	"testing"

	"github.com/maxogod/distro-tp/src/common/logger"
	"github.com/maxogod/distro-tp/src/common/models/raw"
	"github.com/maxogod/distro-tp/src/common/models/reduced"
	disk_storage "github.com/maxogod/distro-tp/src/common/worker/storage/disk_memory"
	"github.com/maxogod/distro-tp/src/joiner/business"
	"github.com/maxogod/distro-tp/src/joiner/cache"
	"github.com/stretchr/testify/assert"
)

// ====== INPUT DATA ======

var TotalItemSumData = []*reduced.TotalSumItem{
	{
		ItemId:    "item1",
		YearMonth: "2024-06",
		Subtotal:  10.0,
		Quantity:  10,
	},
	{
		ItemId:    "item2",
		YearMonth: "2024-07",
		Subtotal:  20.0,
		Quantity:  20,
	},
	{
		ItemId:    "item1",
		YearMonth: "2024-06",
		Subtotal:  15.0,
		Quantity:  15,
	},
}

var TPVData = []*reduced.TotalPaymentValue{
	{
		StoreId:     "store1",
		Semester:    "2024-H1",
		FinalAmount: 1000.0,
	},
	{
		StoreId:     "store2",
		Semester:    "2024-H1",
		FinalAmount: 2000.0,
	},
	{
		StoreId:     "store1",
		Semester:    "2024-H1",
		FinalAmount: 1000.0,
	},
	{
		StoreId:     "store2",
		Semester:    "2024-H1",
		FinalAmount: 2000.0,
	},
}

var CountedUserTransactionsData = []*reduced.CountedUserTransactions{
	{
		StoreId:             "store1",
		UserId:              "1",
		TransactionQuantity: 5,
	},
	{
		StoreId:             "store2",
		UserId:              "2",
		TransactionQuantity: 10,
	},
	{
		StoreId:             "store1",
		UserId:              "1",
		TransactionQuantity: 3,
	},
	{
		StoreId:             "store2",
		UserId:              "150",
		TransactionQuantity: 7,
	},
}

// ====== Reference Data ======
var MenuItemsList = []*raw.MenuItem{
	{ItemId: "item1", ItemName: "Pizza"},
	{ItemId: "item2", ItemName: "Burger"},
}

var StoresList = []*raw.Store{
	{StoreId: "store1", StoreName: "Main Street Store"},
	{StoreId: "store2", StoreName: "Downtown Store"},
}

var UsersList = []*raw.User{
	{UserId: "1", Birthdate: "08/10/2000"},
	{UserId: "2", Birthdate: "12/05/1995"},
	{UserId: "150", Birthdate: "23/03/1988"},
}

// ====== Reference Data Maps ======

var MenuItems = map[string]*raw.MenuItem{
	"item1": {ItemName: "Pizza"},
	"item2": {ItemName: "Burger"},
}

var Stores = map[string]*raw.Store{
	"store1": {StoreName: "Main Street Store"},
	"store2": {StoreName: "Downtown Store"},
}

var Users = map[string]*raw.User{
	"1":   {Birthdate: "08/10/2000"},
	"2":   {Birthdate: "12/05/1995"},
	"150": {Birthdate: "23/03/1988"},
}

func TestJoinTotalSumItems(t *testing.T) {
	logger.InitLogger(logger.LoggerEnvDevelopment)
	cache_service := cache.NewInMemoryCache()
	storage_service := disk_storage.NewDiskMemoryStorage()
	service := business.NewJoinerService(cache_service, storage_service, 100)

	clientID := "client1"
	defer service.DeleteClientRefData(clientID)

	service.StoreMenuItems(clientID, MenuItemsList)
	service.FinishStoringRefData(clientID)

	for _, item := range TotalItemSumData {

		originalId := item.ItemId

		err := service.JoinTotalSumItem(item, clientID)

		assert.NoError(t, err)
		assert.Equal(t, MenuItems[originalId].ItemName, item.ItemId)
	}
}

func TestJoinTotalPaymentValueData(t *testing.T) {
	logger.InitLogger(logger.LoggerEnvDevelopment)
	cache_service := cache.NewInMemoryCache()
	storage_service := disk_storage.NewDiskMemoryStorage()
	service := business.NewJoinerService(cache_service, storage_service, 100)

	clientID := "client2"
	defer service.DeleteClientRefData(clientID)

	service.StoreShops(clientID, StoresList)
	service.FinishStoringRefData(clientID)

	for _, item := range TPVData {

		originalId := item.StoreId

		err := service.JoinTotalPaymentValue(item, clientID)

		assert.NoError(t, err)
		assert.Equal(t, Stores[originalId].StoreName, item.StoreId)
	}
}

func TestJoinCountedUserTransactionData(t *testing.T) {
	logger.InitLogger(logger.LoggerEnvDevelopment)
	cache_service := cache.NewInMemoryCache()
	storage_service := disk_storage.NewDiskMemoryStorage()
	service := business.NewJoinerService(cache_service, storage_service, 100)

	clientID := "client3"
	defer service.DeleteClientRefData(clientID)

	service.StoreShops(clientID, StoresList)
	service.StoreUsers(clientID, UsersList)
	service.FinishStoringRefData(clientID)

	for _, item := range CountedUserTransactionsData {

		originalStoreId := item.StoreId
		originalUserId := item.UserId

		err := service.JoinCountedUserTransactions(item, clientID)

		assert.NoError(t, err)
		assert.Equal(t, Stores[originalStoreId].StoreName, item.StoreId)
		assert.Equal(t, Users[originalUserId].Birthdate, item.Birthdate)
	}
}

func TestDontJoinUntilAllDataIsPresent(t *testing.T) {
	logger.InitLogger(logger.LoggerEnvDevelopment)
	cache_service := cache.NewInMemoryCache()
	storage_service := disk_storage.NewDiskMemoryStorage()
	service := business.NewJoinerService(cache_service, storage_service, 100)

	clientID := "client1"
	defer service.DeleteClientRefData(clientID)

	service.StoreMenuItems(clientID, MenuItemsList)

	assert.Error(t, service.JoinTotalSumItem(TotalItemSumData[0], clientID))

	service.FinishStoringRefData(clientID)

	for _, item := range TotalItemSumData {

		originalId := item.ItemId

		err := service.JoinTotalSumItem(item, clientID)

		assert.NoError(t, err)
		assert.Equal(t, MenuItems[originalId].ItemName, item.ItemId)
	}
}
