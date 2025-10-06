package config

import (
	"fmt"
	"strings"

	"github.com/maxogod/distro-tp/src/filter/internal/handler"
	"github.com/pkg/errors"
	"github.com/spf13/viper"
)

type Config struct {
	Address    string
	LogLevel   string
	TaskConfig handler.TaskConfig
}

func (c Config) String() string {
	return fmt.Sprintf(
		"Address: %s | LogLevel: %s | TaskConfig: [%s]",
		c.Address,
		c.LogLevel,
		c.TaskConfig.String(),
	)
}

const CONFIG_FILE_PATH = "./config.yaml"

func InitConfig() (*Config, error) {

	v := viper.New()
	v.SetEnvKeyReplacer(strings.NewReplacer(".", "_"))
	v.AutomaticEnv()

	v.SetConfigFile(CONFIG_FILE_PATH)
	if err := v.ReadInConfig(); err != nil {
		return nil, errors.Wrapf(err, "failed to read config file %s", CONFIG_FILE_PATH)
	}

	config := &Config{
		Address:  v.GetString("gateway.address"),
		LogLevel: v.GetString("log.level"),
		TaskConfig: handler.TaskConfig{
			FilterYearFrom:       v.GetInt("filter.year.from"),
			FilterYearTo:         v.GetInt("filter.year.to"),
			BusinessHourFrom:     v.GetInt("filter.businessHours.from"),
			BusinessHourTo:       v.GetInt("filter.businessHours.to"),
			TotalAmountThreshold: v.GetFloat64("filter.totalAmountThreshold"),
		},
	}

	return config, nil
}
