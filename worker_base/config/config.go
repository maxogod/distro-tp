package config

import (
	"fmt"
	"os"
	"strings"

	"github.com/op/go-logging"
	"github.com/pkg/errors"
	"github.com/spf13/viper"
)

type Config struct {
	GatewayAddress string
	GatewayUser    string
	GatewayPass    string
	LogLevel       string
}

func (c Config) String() string {
	return fmt.Sprintf(
		"GatewayAddress: %s | GatewayUser: %s | GatewayPass: %s | LogLevel: %s",
		c.GatewayAddress,
		c.GatewayUser,
		c.GatewayPass,
		c.LogLevel,
	)
}

const CONFIG_FILE_PATH = "/config.yaml"

func InitConfig() (*Config, error) {

	v := viper.New()
	v.SetEnvKeyReplacer(strings.NewReplacer(".", "_"))
	v.AutomaticEnv()

	v.SetConfigFile(CONFIG_FILE_PATH)
	if err := v.ReadInConfig(); err != nil {
		return nil, errors.Wrapf(err, "failed to read config file %s", CONFIG_FILE_PATH)
	}

	config := &Config{
		GatewayAddress: v.GetString("gateway.address"),
		GatewayUser:    v.GetString("gateway.name"),
		GatewayPass:    v.GetString("gateway.password"),
		LogLevel:       v.GetString("log.level"),
	}

	return config, nil
}

func InitLogger(logLevel string) error {
	baseBackend := logging.NewLogBackend(os.Stdout, "", 0)
	format := logging.MustStringFormatter(
		`%{time:2006-01-02 15:04:05} %{level:.5s}     %{message}`,
	)
	backendFormatter := logging.NewBackendFormatter(baseBackend, format)

	backendLeveled := logging.AddModuleLevel(backendFormatter)
	logLevelCode, err := logging.LogLevel(logLevel)
	if err != nil {
		return err
	}
	backendLeveled.SetLevel(logLevelCode, "")

	logging.SetBackend(backendLeveled)
	return nil
}
