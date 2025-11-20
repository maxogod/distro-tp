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

	// Verify each item
	for i, item := range menuMock {
		expectedItem := menuMock[i]

		retrievedItem, err := cacheService.GetMenuItem(clientID, item.ItemId)
		assert.NoError(t, err, "Retrieving menu item should not produce an error")
		assert.Equal(t, expectedItem.ItemName, retrievedItem.ItemName, "Menu item name should match")
	}
}
func TestStoreAndGetShops(t *testing.T) {
	clientID := "client2"

	cacheService.StoreShops(clientID, shopsMock)

	for i, item := range shopsMock {
		expectedItem := shopsMock[i]

		retrievedItem, err := cacheService.GetShop(clientID, item.StoreId)
		assert.NoError(t, err, "Retrieving shop should not produce an error")
		assert.Equal(t, expectedItem.StoreName, retrievedItem.StoreName, "Shop name should match")
	}
}

func TestStoreAndGetUsers(t *testing.T) {
	clientID := "client3"

	cacheService.StoreUsers(clientID, usersMock)

	for i, item := range usersMock {
		expectedItem := usersMock[i]

		retrievedItem, err := cacheService.GetUser(clientID, item.UserId)
		assert.NoError(t, err, "Retrieving user should not produce an error")
		assert.Equal(t, expectedItem.Birthdate, retrievedItem.Birthdate, "User birthdate should match")
	}
}

func TestGetMenuItemNotFound(t *testing.T) {
	clientID := "nonexistent"

	_, err := cacheService.GetMenuItem(clientID, "NON EXISTING ITEM")
	assert.Error(t, err, "Retrieving menu items for non-existent client should produce an error")
}

func TestStoreMultipleReferences(t *testing.T) {

	clientID := "client_multi"

	cacheService.StoreMenuItems(clientID, menuMock)
	cacheService.StoreShops(clientID, shopsMock)
	cacheService.StoreUsers(clientID, usersMock)

	for i, item := range shopsMock {
		expectedItem := shopsMock[i]

		retrievedItem, err := cacheService.GetShop(clientID, item.StoreId)
		assert.NoError(t, err, "Retrieving shop should not produce an error")
		assert.Equal(t, expectedItem.StoreName, retrievedItem.StoreName, "Shop name should match")
	}
	for i, item := range usersMock {
		expectedItem := usersMock[i]

		retrievedItem, err := cacheService.GetUser(clientID, item.UserId)
		assert.NoError(t, err, "Retrieving user should not produce an error")
		assert.Equal(t, expectedItem.Birthdate, retrievedItem.Birthdate, "User birthdate should match")
	}
}

func TestRemoveRefData(t *testing.T) {
	clientID := "client4"

	cacheService.StoreMenuItems(clientID, menuMock)

	menuitem := menuMock[0]

	retrievedMenu, err := cacheService.GetMenuItem(clientID, menuitem.ItemId)
	assert.NoError(t, err, "Retrieving menu items should not produce an error")
	assert.Equal(t, menuitem.ItemName, retrievedMenu.ItemName, "Retrieved menu item name should match requested item name")
	cacheService.RemoveRefData(clientID, enum.MenuItems)

	_, err = cacheService.GetMenuItem(clientID, menuitem.ItemId)

	assert.Error(t, err, "Retrieving menu items after removal should produce an error")
}

func TestRemoveAllRefData(t *testing.T) {
	clientID := "client_all"

	cacheService.StoreMenuItems(clientID, menuMock)
	cacheService.StoreShops(clientID, shopsMock)
	cacheService.StoreUsers(clientID, usersMock)

	menuitem := menuMock[0]
	shopitem := shopsMock[0]
	useritem := usersMock[0]

	retrievedMenu, err := cacheService.GetMenuItem(clientID, menuitem.ItemId)
	assert.NoError(t, err, "Retrieving menu items should not produce an error")
	assert.Equal(t, menuitem.ItemName, retrievedMenu.ItemName, "Retrieved menu item name should match requested item name")

	retrievedShop, err := cacheService.GetShop(clientID, shopitem.StoreId)
	assert.NoError(t, err, "Retrieving shops should not produce an error")
	assert.Equal(t, shopitem.StoreName, retrievedShop.StoreName, "Retrieved shop name should match requested shop name")

	retrievedUser, err := cacheService.GetUser(clientID, useritem.UserId)
	assert.NoError(t, err, "Retrieving users should not produce an error")
	assert.Equal(t, useritem.Birthdate, retrievedUser.Birthdate, "Retrieved user birthdate should match requested user birthdate")

	cacheService.RemoveAllRefData(clientID)

	_, err = cacheService.GetMenuItem(clientID, menuitem.ItemId)
	assert.Error(t, err, "Retrieving menu items after removal should produce an error")

	_, err = cacheService.GetShop(clientID, shopitem.StoreId)
	assert.Error(t, err, "Retrieving shops after removal should produce an error")

	_, err = cacheService.GetUser(clientID, useritem.UserId)
	assert.Error(t, err, "Retrieving users after removal should produce an error")
}

func TestClose(t *testing.T) {
	clientID := "client5"

	cacheService.StoreMenuItems(clientID, menuMock)
	err := cacheService.Close()
	assert.NoError(t, err, "Closing cache service should not produce an error")

	_, err = cacheService.GetMenuItem(clientID, "")
	assert.Error(t, err, "Retrieving data after close should produce an error")
}
