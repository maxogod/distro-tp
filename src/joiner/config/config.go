package config

import (
	"fmt"
	"strings"

	"github.com/pkg/errors"
	"github.com/spf13/viper"
)

type Config struct {
	GatewayAddress              string
	StorePath                   string
	LogLevel                    string
	StoreTPVQueue               string
	TransactionCountedQueue     string
	TransactionSumQueue         string
	UserTransactionsQueue       string
	JoinedTransactionsQueue     string
	JoinedStoresTPVQueue        string
	JoinedUserTransactionsQueue string
	GatewayControllerQueue      string
	GatewayControllerExchange   string
	FinishRoutingKey            string
}

func (c Config) String() string {
	return fmt.Sprintf(
		"GatewayAddress: %s | LogLevel: %s | StorePath: %s | StoreTPVQueue: %s"+
			" | TransactionCountedQueue: %s | TransactionSumQueue: %s | UserTransactionsQueue: %s "+
			"| JoinedTransactionsQueue: %s | JoinedStoresTPVQueue: %s | JoinedUserTransactionsQueue: %s "+
			"| GatewayControllerQueue: %s | GatewayControllerExchange: %s | FinishRoutingKey: %s",
		c.GatewayAddress,
		c.StorePath,
		c.LogLevel,
		c.StoreTPVQueue,
		c.TransactionCountedQueue,
		c.TransactionSumQueue,
		c.UserTransactionsQueue,
		c.JoinedTransactionsQueue,
		c.JoinedStoresTPVQueue,
		c.JoinedUserTransactionsQueue,
		c.GatewayControllerQueue,
		c.GatewayControllerExchange,
		c.FinishRoutingKey,
	)
}

const ConfigFilePath = "/config.yaml"

func InitConfig() (*Config, error) {

	v := viper.New()
	v.SetEnvKeyReplacer(strings.NewReplacer(".", "_"))
	v.AutomaticEnv()

	v.SetConfigFile(ConfigFilePath)
	if err := v.ReadInConfig(); err != nil {
		return nil, errors.Wrapf(err, "failed to read config file %s", ConfigFilePath)
	}

	config := &Config{
		GatewayAddress:              v.GetString("gateway.address"),
		StorePath:                   v.GetString("datasets.path"),
		LogLevel:                    v.GetString("log.level"),
		StoreTPVQueue:               v.GetString("queues.store_tpv_queue"),
		TransactionCountedQueue:     v.GetString("queues.transaction_counted_queue"),
		TransactionSumQueue:         v.GetString("queues.transaction_sum_queue"),
		UserTransactionsQueue:       v.GetString("queues.user_transactions_queue"),
		JoinedTransactionsQueue:     v.GetString("aggregator_queues.joined_transactions_queue"),
		JoinedStoresTPVQueue:        v.GetString("aggregator_queues.joined_stores_tpv_queue"),
		JoinedUserTransactionsQueue: v.GetString("aggregator_queues.joined_user_transactions_queue"),
		GatewayControllerQueue:      v.GetString("queues.gateway_controller_queue"),
		GatewayControllerExchange:   v.GetString("exchanges.gateway_controller_exchange"),
		FinishRoutingKey:            v.GetString("exchanges.finish_routing_key"),
	}

	return config, nil
}
