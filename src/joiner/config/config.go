package config

import (
	"fmt"
	"strings"

	"github.com/pkg/errors"
	"github.com/spf13/viper"
)

type ReferenceDatasets struct {
	MenuItem string
	Store    string
	User     string
}

type Config struct {
	Address            string
	LogLevel           string
	PersistenceEnabled bool
	ReferenceDatasets  ReferenceDatasets
}

func (c Config) String() string {
	return fmt.Sprintf(
		"Address: %s | LogLevel: %s | PersistenceEnabled: %t | ReferenceDatasets: [MenuItem=%s, Store=%s, User=%s]",
		c.Address,
		c.LogLevel,
		c.PersistenceEnabled,
		c.ReferenceDatasets.MenuItem,
		c.ReferenceDatasets.Store,
		c.ReferenceDatasets.User,
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
		ReferenceDatasets: ReferenceDatasets{
			MenuItem: v.GetString("reference_datasets.menu_item"),
			Store:    v.GetString("reference_datasets.store"),
			User:     v.GetString("reference_datasets.user"),
		},
	}

	return config, nil
}
