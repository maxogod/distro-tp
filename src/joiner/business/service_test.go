package business_test

// import (
// 	"testing"

// 	"github.com/maxogod/distro-tp/src/common/models/raw"
// 	"github.com/maxogod/distro-tp/src/common/models/reduced"
// 	"github.com/maxogod/distro-tp/src/joiner/business"
// 	"github.com/maxogod/distro-tp/src/joiner/cache"
// 	"github.com/stretchr/testify/assert"
// )

// func TestJoinTotalProfitBySubtotal(t *testing.T) {
// 	cache := cache.NewInMemoryCache()
// 	service := business.NewJoinerService(cache)
// 	menuItem := &raw.MenuItem{ItemId: "item1", ItemName: "Pizza"}
// 	service.StoreMenuItems("client1", []*raw.MenuItem{menuItem})
// 	service.FinishStoringRefData("client1")
// 	profit := &reduced.TotalProfitBySubtotal{
// 		ItemId: "item1",
// 	}
// 	result := service.JoinTotalProfitBySubtotal(profit, "client1")
// 	assert.NotNil(t, result)
// 	assert.Equal(t, "Pizza", result[0].ItemId)
// }

// func TestJoinTotalSoldByQuantity(t *testing.T) {
// 	cache := cache.NewInMemoryCache()
// 	service := business.NewJoinerService(cache)
// 	menuItem := &raw.MenuItem{ItemId: "item2", ItemName: "Burger"}
// 	service.StoreMenuItems("client1", []*raw.MenuItem{menuItem})
// 	service.FinishStoringRefData("client1")
// 	sales := &reduced.TotalSoldByQuantity{
// 		ItemId: "item2",
// 	}
// 	result := service.JoinTotalSoldByQuantity(sales, "client1")
// 	assert.NotNil(t, result)
// 	assert.Equal(t, "Burger", result[0].ItemId)
// }

// func TestJoinTotalPaymentValue(t *testing.T) {
// 	cache := cache.NewInMemoryCache()
// 	service := business.NewJoinerService(cache)
// 	service.FinishStoringRefData("client1")
// 	store := &raw.Store{StoreId: "store1", StoreName: "Main Store"}
// 	service.StoreShops("client1", []*raw.Store{store})
// 	tpv := &reduced.TotalPaymentValue{
// 		StoreId: "store1",
// 	}
// 	result := service.JoinTotalPaymentValue(tpv, "client1")
// 	assert.NotNil(t, result)
// 	assert.Equal(t, "Main Store", result[0].StoreId)
// }

// func TestJoinCountedUserTransactions(t *testing.T) {
// 	cache := cache.NewInMemoryCache()
// 	service := business.NewJoinerService(cache)
// 	service.FinishStoringRefData("client1")
// 	store := &raw.Store{StoreId: "store1", StoreName: "Main Store"}
// 	user := &raw.User{UserId: "user1", Birthdate: "2000-01-01"}
// 	service.StoreShops("client1", []*raw.Store{store})
// 	service.StoreUsers("client1", []*raw.User{user})
// 	tx := &reduced.CountedUserTransactions{
// 		StoreId: "store1",
// 		UserId:  "user1",
// 	}
// 	result := service.JoinCountedUserTransactions(tx, "client1")
// 	assert.NotNil(t, result)
// }

// func TestBufferedT2_1Join(t *testing.T) {
// 	cache := cache.NewInMemoryCache()
// 	service := business.NewJoinerService(cache)

// 	menuItems := []*raw.MenuItem{
// 		{ItemId: "item1", ItemName: "Pizza"},
// 		{ItemId: "item2", ItemName: "Burger"},
// 	}

// 	service.StoreMenuItems("client1", menuItems)

// 	profit := &reduced.TotalProfitBySubtotal{
// 		ItemId: "item1",
// 	}
// 	// If i join BEFORE finishing storing ref data, it should be buffered
// 	result := service.JoinTotalProfitBySubtotal(profit, "client1")
// 	assert.Nil(t, result) // because its buffered

// 	// Now finish storing ref data
// 	service.FinishStoringRefData("client1")

// 	profit2 := &reduced.TotalProfitBySubtotal{
// 		ItemId: "item2",
// 	}
// 	// If i join AFTER
// 	result2 := service.JoinTotalProfitBySubtotal(profit2, "client1")
// 	assert.NotNil(t, result2)
// 	t.Log(result2)
// 	assert.Equal(t, 2, len(result2))
// 	assert.Equal(t, "Burger", result2[0].ItemId)
// 	assert.Equal(t, "Pizza", result2[1].ItemId)
// }

// func TestBufferedT2_2Join(t *testing.T) {
// 	cache := cache.NewInMemoryCache()
// 	service := business.NewJoinerService(cache)

// 	menuItems := []*raw.MenuItem{
// 		{ItemId: "item1", ItemName: "Pizza"},
// 		{ItemId: "item2", ItemName: "Burger"},
// 	}

// 	service.StoreMenuItems("client1", menuItems)

// 	quantity := &reduced.TotalSoldByQuantity{
// 		ItemId: "item1",
// 	}
// 	// If i join BEFORE finishing storing ref data, it should be buffered
// 	result := service.JoinTotalSoldByQuantity(quantity, "client1")
// 	assert.Nil(t, result) // because its buffered

// 	// Now finish storing ref data
// 	service.FinishStoringRefData("client1")

// 	quantity2 := &reduced.TotalSoldByQuantity{
// 		ItemId: "item2",
// 	}
// 	// If i join AFTER
// 	result2 := service.JoinTotalSoldByQuantity(quantity2, "client1")
// 	assert.NotNil(t, result2)
// 	t.Log(result2)
// 	assert.Equal(t, 2, len(result2))
// 	assert.Equal(t, "Burger", result2[0].ItemId)
// 	assert.Equal(t, "Pizza", result2[1].ItemId)
// }

// func TestBufferedT3Join(t *testing.T) {
// 	cache := cache.NewInMemoryCache()
// 	service := business.NewJoinerService(cache)

// 	stores := []*raw.Store{
// 		{StoreId: "store1", StoreName: "Store one"},
// 		{StoreId: "store2", StoreName: "Store two"},
// 	}

// 	service.StoreShops("client1", stores)

// 	tpv := &reduced.TotalPaymentValue{
// 		StoreId: "store1",
// 	}
// 	// If i join BEFORE finishing storing ref data, it should be buffered
// 	result := service.JoinTotalPaymentValue(tpv, "client1")
// 	assert.Nil(t, result) // because its buffered

// 	// Now finish storing ref data
// 	service.FinishStoringRefData("client1")

// 	tpv2 := &reduced.TotalPaymentValue{
// 		StoreId: "store2",
// 	}
// 	// If i join AFTER
// 	result2 := service.JoinTotalPaymentValue(tpv2, "client1")
// 	assert.NotNil(t, result2)
// 	t.Log(result2)
// 	assert.Equal(t, 2, len(result2))
// 	assert.Equal(t, "Store two", result2[0].StoreId)
// 	assert.Equal(t, "Store one", result2[1].StoreId)
// }

// func TestBufferedT4Join(t *testing.T) {
// 	cache := cache.NewInMemoryCache()
// 	service := business.NewJoinerService(cache)

// 	stores := []*raw.Store{
// 		{StoreId: "store1", StoreName: "Store one"},
// 		{StoreId: "store2", StoreName: "Store two"},
// 	}

// 	users := []*raw.User{
// 		{UserId: "user1", Birthdate: "2000-01-01"},
// 		{UserId: "user2", Birthdate: "2000-02-02"},
// 	}

// 	service.StoreShops("client1", stores)
// 	service.StoreUsers("client1", users)

// 	countedUser := &reduced.CountedUserTransactions{
// 		StoreId: "store1",
// 		UserId:  "user1",
// 	}
// 	// If i join BEFORE finishing storing ref data, it should be buffered
// 	result := service.JoinCountedUserTransactions(countedUser, "client1")
// 	assert.Nil(t, result) // because its buffered

// 	// Now finish storing ref data
// 	service.FinishStoringRefData("client1")

// 	countedUser2 := &reduced.CountedUserTransactions{
// 		StoreId: "store2",
// 		UserId:  "user2",
// 	}
// 	// If i join AFTER
// 	result2 := service.JoinCountedUserTransactions(countedUser2, "client1")
// 	assert.NotNil(t, result2)
// 	t.Log(result2)
// }
