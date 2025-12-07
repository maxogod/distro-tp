package config

import (
	"fmt"
	"strings"
	"time"

	"github.com/maxogod/distro-tp/src/filter/internal/task_executor"
	"github.com/pkg/errors"
	"github.com/spf13/viper"
)

type HeartbeatConfig struct {
	Host     string
	Port     int
	Amount   int
	Interval time.Duration
}

func (h HeartbeatConfig) String() string {
	return fmt.Sprintf(
		"Host: %s | Port: %d | Amount: %d | Interval(ms): %d",
		h.Host,
		h.Port,
		h.Amount,
		h.Interval.Milliseconds(),
	)
}

type Config struct {
	ID         string
	Address    string
	LogLevel   string
	TaskConfig task_executor.TaskConfig
	Heartbeat  HeartbeatConfig
}

func (c Config) String() string {
	return fmt.Sprintf(
		"Address: %s | LogLevel: %s | TaskConfig: [%s] | Heartbeat: [%s]",
		c.Address,
		c.LogLevel,
		c.TaskConfig.String(),
		c.Heartbeat.String(),
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

	v.BindEnv("id", "ID")
	v.BindEnv("heartbeat.amount", "EOL_AMOUNT")

	heatbeatConf := HeartbeatConfig{
		Host:     v.GetString("heartbeat.host"),
		Port:     v.GetInt("heartbeat.port"),
		Amount:   v.GetInt("heartbeat.amount"),
		Interval: time.Duration(v.GetInt("heartbeat.interval")) * time.Millisecond,
	}

	config := &Config{
		ID:       v.GetString("id"),
		Address:  v.GetString("gateway.address"),
		LogLevel: v.GetString("log.level"),
		TaskConfig: task_executor.TaskConfig{
			FilterYearFrom:       v.GetInt("filter.year.from"),
			FilterYearTo:         v.GetInt("filter.year.to"),
			BusinessHourFrom:     v.GetInt("filter.businessHours.from"),
			BusinessHourTo:       v.GetInt("filter.businessHours.to"),
			TotalAmountThreshold: v.GetFloat64("filter.totalAmountThreshold"),
		},
		Heartbeat: heatbeatConf,
	}

	return config, nil
}
