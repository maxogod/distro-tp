package config

import (
	"fmt"
	"strings"

	"github.com/pkg/errors"
	"github.com/spf13/viper"
)

type Config struct {
	GatewayAddress                     string
	StorePath                          string
	BatchSize                          int
	LogLevel                           string
	FilteredTransactionsQueue          string
	JoinedMostProfitsTransactionsQueue string
	JoinedBestSellingTransactionsQueue string
	JoinedStoresTPVQueue               string
	JoinedUserTransactionsQueue        string
}

func (c Config) String() string {
	return fmt.Sprintf(
		"GatewayAddress: %s | LogLevel: %s | StorePath: %s | BatchSize: %d | FilteredTransactionsQueue: %s "+
			"| JoinedMostProfitsTransactionsQueue: %s | JoinedBestSellingTransactionsQueue: %s "+
			"| JoinedStoresTPVQueue: %s | JoinedUserTransactionsQueue: %s "+
			c.GatewayAddress,
		c.LogLevel,
		c.StorePath,
		c.BatchSize,
		c.FilteredTransactionsQueue,
		c.JoinedMostProfitsTransactionsQueue,
		c.JoinedBestSellingTransactionsQueue,
		c.JoinedStoresTPVQueue,
		c.JoinedUserTransactionsQueue,
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
		GatewayAddress:                     v.GetString("gateway.address"),
		StorePath:                          v.GetString("datasets.path"),
		BatchSize:                          v.GetInt("datasets.batch.size"),
		LogLevel:                           v.GetString("log.level"),
		FilteredTransactionsQueue:          v.GetString("queues.filtered_transactions_queue"),
		JoinedMostProfitsTransactionsQueue: v.GetString("queues.joined_most_profits_transactions_queue"),
		JoinedBestSellingTransactionsQueue: v.GetString("queues.joined_best_selling_transactions_queue"),
		JoinedStoresTPVQueue:               v.GetString("queues.joined_stores_tpv_queue"),
		JoinedUserTransactionsQueue:        v.GetString("queues.joined_user_transactions_queue"),
	}

	return config, nil
}
