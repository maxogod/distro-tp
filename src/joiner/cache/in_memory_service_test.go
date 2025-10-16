package cache_test

import (
	"testing"

	"github.com/maxogod/distro-tp/src/common/models/raw"
	"github.com/maxogod/distro-tp/src/joiner/cache"
	"github.com/stretchr/testify/assert"
	"google.golang.org/protobuf/proto"
)

var cacheService cache.CacheService

var menuMock = []*raw.MenuItem{
	{ItemId: "item1", ItemName: "Burger"},
	{ItemId: "item2", ItemName: "Fries"},
	{ItemId: "item3", ItemName: "Soda"},
}

var transactionItemsMock = []*raw.TransactionItem{
	{ItemId: "item1", Quantity: 1},
	{ItemId: "item2", Quantity: 2},
	{ItemId: "item3", Quantity: 3},
}

// TestMain sets up the in-memory cache service and mock data before running the tests.
func TestMain(m *testing.M) {
	cacheService = cache.NewInMemoryCache()

	cacheService.StoreRefData("sharedClient", "sharedItem", menuMock[0])

	m.Run()
}

// TestStoreAndGetRefData tests the success case for the store and retrieve reference data methods.
func TestStoreAndGetRefData(t *testing.T) {
	for _, item := range menuMock {
		refID := item.ItemId + "@" + "menu_item"
		err := cacheService.StoreRefData("client1", refID, item)
		assert.NoError(t, err)
	}

	for _, item := range menuMock {
		refID := item.ItemId + "@" + "menu_item"
		retrieved, err := cacheService.GetRefData("client1", refID)
		assert.NoError(t, err)
		assert.NotNil(t, retrieved)
		retrievedItem, ok := retrieved.(*raw.MenuItem)
		assert.True(t, ok)
		assert.Equal(t, item.ItemId, retrievedItem.ItemId)
		assert.Equal(t, item.ItemName, retrievedItem.ItemName)
	}
}

// TestStoreAndGetRefData_Errors tests the error cases for the store and retrieve reference data methods.
func TestStoreAndGetRefData_Errors(t *testing.T) {
	// Attempt to retrieve data for a non-existent client
	retrieved, err := cacheService.GetRefData("nonExistentClient", "someRefID")
	assert.Error(t, err)
	assert.Nil(t, retrieved)

	// Attempt to retrieve data for a non-existent reference ID
	retrieved, err = cacheService.GetRefData("sharedClient", "nonExistentRefID")
	assert.Error(t, err)
	assert.Nil(t, retrieved)

	// Attempt to store data with an empty client ID (create if not exists behavior)
	err = cacheService.StoreRefData("", "someRefID", menuMock[0])
	assert.NoError(t, err)
	retrieved, err = cacheService.GetRefData("", "someRefID")
	assert.NoError(t, err)
	assert.NotNil(t, retrieved)
}

// TestBufferAndGetData tests the success case for the buffer and retrieve unreferenced data methods.
func TestBufferAndGetData(t *testing.T) {
	clientID := "client2"
	bufferID := "bufferClient2"
	for _, item := range transactionItemsMock {
		err := cacheService.BufferUnreferencedData(clientID, bufferID, item)
		assert.NoError(t, err)
	}

	for _, item := range menuMock {
		refID := item.ItemId + "@" + "menu_item"
		err := cacheService.StoreRefData("client2", refID, item)
		assert.NoError(t, err)
	}

	collected := []*raw.TransactionItem{}
	err := cacheService.IterateUnreferencedData(clientID, bufferID, func(m proto.Message) bool {
		ti, ok := m.(*raw.TransactionItem)
		assert.True(t, ok)

		refID := ti.ItemId + "@" + "menu_item"
		retrieved, err := cacheService.GetRefData(clientID, refID)
		assert.NoError(t, err)
		menuItem, ok := retrieved.(*raw.MenuItem)
		assert.True(t, ok)
		assert.Equal(t, ti.ItemId, menuItem.ItemId)
		assert.NotEmpty(t, menuItem.ItemName)
		ti.ItemId = menuItem.ItemName // Join operation

		collected = append(collected, ti)
		return true // Remove after processing
	})
	assert.NoError(t, err)
	assert.Equal(t, len(transactionItemsMock), len(collected))

	for _, item := range collected {
		switch item.Quantity {
		case 1:
			assert.Equal(t, "Burger", item.ItemId)
		case 2:
			assert.Equal(t, "Fries", item.ItemId)
		case 3:
			assert.Equal(t, "Soda", item.ItemId)
		default:
			t.Errorf("Unexpected quantity: %d", item.Quantity)
		}
	}
}

// TestBufferAndGetData_Errors tests the error cases for the buffer and retrieve unreferenced data methods.
func TestBufferAndGetData_Errors(t *testing.T) {
	clientID := "client3"
	bufferID := "bufferClient3"

	// Buffer data and read without reference data stored
	for _, item := range transactionItemsMock {
		err := cacheService.BufferUnreferencedData(clientID, bufferID, item)
		assert.NoError(t, err)
	}
	err := cacheService.IterateUnreferencedData(clientID, bufferID, func(m proto.Message) bool {
		ti, ok := m.(*raw.TransactionItem)
		assert.True(t, ok)
		assert.NotEmpty(t, ti.ItemId)

		refID := ti.ItemId + "@" + "menu_item"
		retrieved, err := cacheService.GetRefData(clientID, refID)
		assert.Error(t, err)
		assert.Nil(t, retrieved)

		return true // Remove after processing
	})
	assert.NoError(t, err)

	// Inexistent clientID
	err = cacheService.IterateUnreferencedData("nonExistentClient", bufferID, func(m proto.Message) bool {
		panic("Should not be called")
	})
	assert.Error(t, err)

	// Inexistent bufferID
	err = cacheService.IterateUnreferencedData(clientID, "nonExistentBuffer", func(m proto.Message) bool {
		panic("Should not be called")
	})
	assert.NoError(t, err)
}

// TestCantJoinWithOtherClientData tests that data from one client cannot be accessed by another client.
// Also tests that if callback returns false, data is not removed.
func TestCantJoinWithOtherClientData(t *testing.T) {
	clientRefID := "client4"
	clientBuffID := "client4-prime"
	bufferID := "bufferClient4-prime"

	// Buffer data and read without reference data stored
	for _, item := range transactionItemsMock {
		err := cacheService.BufferUnreferencedData(clientBuffID, bufferID, item)
		assert.NoError(t, err)
	}

	for _, item := range menuMock {
		refID := item.ItemId + "@" + "menu_item"
		err := cacheService.StoreRefData(clientRefID, refID, item)
		assert.NoError(t, err)
	}

	// Try reading twice, and ensure buff data is still there (not removed)
	for range 2 {
		err := cacheService.IterateUnreferencedData(clientBuffID, bufferID, func(m proto.Message) bool {
			ti, ok := m.(*raw.TransactionItem)
			assert.True(t, ok)
			assert.NotEmpty(t, ti.ItemId)

			refID := ti.ItemId + "@" + "menu_item"
			retrieved, err := cacheService.GetRefData(clientBuffID, refID)
			assert.Error(t, err)
			assert.Nil(t, retrieved)

			return false // Dont remove
		})
		assert.NoError(t, err)
	}
}

// TestClearClientData tests that the data cant be accessed after being cleared.
func TestClearClientData(t *testing.T) {
	clientID := "client5"
	bufferID := "bufferClient5"

	// Buffer data
	for _, item := range transactionItemsMock {
		err := cacheService.BufferUnreferencedData(clientID, bufferID, item)
		assert.NoError(t, err)
	}

	// Store reference data
	for _, item := range menuMock {
		refID := item.ItemId + "@" + "menu_item"
		err := cacheService.StoreRefData(clientID, refID, item)
		assert.NoError(t, err)
	}

	// Remove client data REF + BUFF
	cacheService.RemoveRefData(clientID)

	err := cacheService.IterateUnreferencedData(clientID, bufferID, func(m proto.Message) bool {
		panic("Should not be called")
	})
	assert.Error(t, err)
}
