package config

import (
	"fmt"
	"strings"

	"github.com/pkg/errors"
	"github.com/spf13/viper"
)

type Limits struct {
	TransactionSendLimit int32
	MaxAmountToSend      int
}

type Config struct {
	Address            string
	LogLevel           string
	PersistenceEnabled bool
	Limits             Limits
}

func (c Config) String() string {
	return fmt.Sprintf(
		"Address: %s | LogLevel: %s | PersistenceEnabled: %t | Limits: [TransactionSendLimit=%d, MaxAmountToSend=%d]",
		c.Address,
		c.LogLevel,
		c.PersistenceEnabled,
		c.Limits.TransactionSendLimit,
		c.Limits.MaxAmountToSend,
	)
}

const CONFIG_FILE_PATH = "./config.yaml"

func InitConfig(configFilePath string) (*Config, error) {
	v := viper.New()
	v.SetEnvKeyReplacer(strings.NewReplacer(".", "_"))
	v.AutomaticEnv()

	configFile := CONFIG_FILE_PATH
	if configFilePath != "" {
		configFile = configFilePath
	}

	v.SetConfigFile(configFile)
	if err := v.ReadInConfig(); err != nil {
		return nil, errors.Wrapf(err, "failed to read config file %s", configFile)
	}

	config := &Config{
		Address:            v.GetString("gateway.address"),
		LogLevel:           v.GetString("log.level"),
		PersistenceEnabled: v.GetBool("persistence.enabled"),
		Limits: Limits{
			TransactionSendLimit: v.GetInt32("limits.transaction_send_limit"),
			MaxAmountToSend:      v.GetInt("limits.max_amount_to_send"),
		},
	}

	return config, nil
}
