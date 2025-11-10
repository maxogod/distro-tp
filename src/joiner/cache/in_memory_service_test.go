package cache_test

import (
	"testing"

	"github.com/maxogod/distro-tp/src/common/models/enum"
	"github.com/maxogod/distro-tp/src/common/models/raw"
	"github.com/maxogod/distro-tp/src/joiner/cache"
	"github.com/stretchr/testify/assert"
)

var cacheService cache.InMemoryService

var menuMock = []*raw.MenuItem{
	{ItemId: "item1", ItemName: "Burger"},
	{ItemId: "item2", ItemName: "Fries"},
	{ItemId: "item3", ItemName: "Soda"},
}

var shopsMock = []*raw.Store{
	{StoreId: "store1", StoreName: "FastFood Inc."},
	{StoreId: "store2", StoreName: "Healthy Eats"},
}

var usersMock = []*raw.User{
	{UserId: "user1", Birthdate: "08/10/2000"},
	{UserId: "user2", Birthdate: "20/06/2004"},
}

// TestMain sets up the in-memory cache service and mock data before running the tests.
func TestMain(m *testing.M) {

	cacheService = cache.NewInMemoryCache()
	m.Run()
}

// TestStoreAndGetRefData tests the success case for the store and retrieve reference data methods.
func TestStoreAndGetMenuItems(t *testing.T) {
	clientID := "client1"

	// Store menu items
	cacheService.StoreMenuItems(clientID, menuMock)

	// Retrieve menu items
	retrievedMenu, err := cacheService.GetMenuItem(clientID)
	assert.NoError(t, err, "Retrieving menu items should not produce an error")
	assert.Equal(t, len(menuMock), len(retrievedMenu), "Number of retrieved menu items should match stored items")

	// Verify each item
	for _, item := range menuMock {
		retrievedItem, exists := retrievedMenu[item.ItemId]
		assert.True(t, exists, "Menu item should exist in retrieved data")
		assert.Equal(t, item.ItemName, retrievedItem.ItemName, "Menu item name should match")
	}
}
func TestStoreAndGetShops(t *testing.T) {
	clientID := "client2"

	cacheService.StoreShops(clientID, shopsMock)

	retrievedShops, err := cacheService.GetShop(clientID)
	assert.NoError(t, err, "Retrieving shops should not produce an error")
	assert.Equal(t, len(shopsMock), len(retrievedShops), "Number of retrieved shops should match stored items")

	for _, item := range shopsMock {
		retrievedItem, exists := retrievedShops[item.StoreId]
		assert.True(t, exists, "Shop should exist in retrieved data")
		assert.Equal(t, item.StoreName, retrievedItem.StoreName, "Shop name should match")
	}
}

func TestStoreAndGetUsers(t *testing.T) {
	clientID := "client3"

	cacheService.StoreUsers(clientID, usersMock)

	retrievedUsers, err := cacheService.GetUser(clientID)
	assert.NoError(t, err, "Retrieving users should not produce an error")
	assert.Equal(t, len(usersMock), len(retrievedUsers), "Number of retrieved users should match stored items")

	for _, item := range usersMock {
		retrievedItem, exists := retrievedUsers[item.UserId]
		assert.True(t, exists, "User should exist in retrieved data")
		assert.Equal(t, item.Birthdate, retrievedItem.Birthdate, "User birthdate should match")
	}
}

func TestGetMenuItemNotFound(t *testing.T) {
	clientID := "nonexistent"

	_, err := cacheService.GetMenuItem(clientID)
	assert.Error(t, err, "Retrieving menu items for non-existent client should produce an error")
}

func TestStoreMultipleReferences(t *testing.T) {

	clientID := "client_multi"

	cacheService.StoreMenuItems(clientID, menuMock)
	cacheService.StoreShops(clientID, shopsMock)
	cacheService.StoreUsers(clientID, usersMock)

	retrievedMenu, err := cacheService.GetMenuItem(clientID)
	assert.NoError(t, err, "Retrieving menu items should not produce an error")
	assert.Equal(t, len(menuMock), len(retrievedMenu), "Number of retrieved menu items should match stored items")

	retrievedShops, err := cacheService.GetShop(clientID)
	assert.NoError(t, err, "Retrieving shops should not produce an error")
	assert.Equal(t, len(shopsMock), len(retrievedShops), "Number of retrieved shops should match stored items")

	retrievedUsers, err := cacheService.GetUser(clientID)
	assert.NoError(t, err, "Retrieving users should not produce an error")
	assert.Equal(t, len(usersMock), len(retrievedUsers), "Number of retrieved users should match stored items")
}

func TestRemoveRefData(t *testing.T) {
	clientID := "client4"

	cacheService.StoreMenuItems(clientID, menuMock)

	retrievedMenu, err := cacheService.GetMenuItem(clientID)
	assert.NoError(t, err, "Retrieving menu items should not produce an error")
	assert.Equal(t, len(menuMock), len(retrievedMenu), "Number of retrieved menu items should match stored items")

	cacheService.RemoveRefData(clientID, enum.MenuItems)

	_, err = cacheService.GetMenuItem(clientID)
	
	assert.Error(t, err, "Retrieving menu items after removal should produce an error")
}

func TestClose(t *testing.T) {
	clientID := "client5"

	cacheService.StoreMenuItems(clientID, menuMock)
	err := cacheService.Close()
	assert.NoError(t, err, "Closing cache service should not produce an error")

	_, err = cacheService.GetMenuItem(clientID)
	assert.Error(t, err, "Retrieving data after close should produce an error")
}
