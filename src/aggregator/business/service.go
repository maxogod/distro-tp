package business

import (
	"github.com/maxogod/distro-tp/src/aggregator/cache"
	"github.com/maxogod/distro-tp/src/common/logger"
	"github.com/maxogod/distro-tp/src/common/models/raw"
	"github.com/maxogod/distro-tp/src/common/models/reduced"
	"google.golang.org/protobuf/proto"
)

var log = logger.GetLogger()

type AggregatorService struct {
	cacheService cache.CacheService
}

func NewAggregatorService(cacheService cache.CacheService) AggregatorService {
	return AggregatorService{
		cacheService: cacheService,
	}
}

// This is T1
func (as *AggregatorService) StoreTransactions(clientID string, transactions []*raw.Transaction) error {

	protoTransactions := make([]*proto.Message, len(transactions))
	for i, tx := range transactions {
		msg := proto.Message(tx)
		protoTransactions[i] = &msg
	}

	return as.cacheService.StoreBatch(clientID, protoTransactions)

}

// This is T2_1
func (as *AggregatorService) StoreTotalProfitBySubtotal(clientID string, reducedData []*reduced.TotalProfitBySubtotal) error {
	return nil
}

// This is T2_2
func (as *AggregatorService) StoreTotalSoldByQuantity(clientID string, reducedData []*reduced.TotalSoldByQuantity) error {
	return nil
}

// This is T3
func (as *AggregatorService) StoreTotalPaymentValue(clientID string, reducedData []*reduced.TotalPaymentValue) error {
	return nil
}

// This is T4
func (as *AggregatorService) StoreCountedUserTransactions(clientID string, reducedData []*reduced.CountedUserTransactions) error {
	return nil
}

func (as *AggregatorService) GetStoredTransactions(clientID string, amount int32) ([]*raw.Transaction, bool) {
	protoMessages, err := as.cacheService.ReadBatch(clientID, amount)
	if err != nil {
		log.Errorf("Error reading batch from cache: %v", err)
		return nil, false
	}

	if len(protoMessages) == 0 {
		return nil, false
	}

	transactions := make([]*raw.Transaction, len(protoMessages))
	for i, msg := range protoMessages {
		transactions[i] = (*msg).(*raw.Transaction)
	}

	return transactions, true
}

func (as *AggregatorService) Close() error {
	return as.cacheService.Close()
}
