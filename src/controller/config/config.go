package config

import (
	"fmt"
	"strings"

	"github.com/joho/godotenv"
	"github.com/spf13/viper"
)

type Config struct {
	MiddlewareAddress string
	Port              int32
	LogLevel          string
	ReceivingTimeout  int
}

func (c Config) String() string {
	return fmt.Sprintf(
		"[CONFIG: Port: %d | LogLevel: %s | ReceivingTimeout: %d]",
		c.Port,
		c.LogLevel,
		c.ReceivingTimeout,
	)
}

const CONFIG_FILE_PATH = "/config.yaml"

func InitConfig() (*Config, error) {
	v := viper.New()
	v.SetEnvKeyReplacer(strings.NewReplacer(".", "_"))

	_ = godotenv.Load(".env")
	v.AutomaticEnv()

	v.SetConfigFile(CONFIG_FILE_PATH)
	_ = v.ReadInConfig() // ignore error if file missing

	// Bind env vars to config keys
	v.BindEnv("middleware.address", "MIDDLEWARE_ADDRESS")
	v.BindEnv("port", "PORT")
	v.BindEnv("log.level", "LOG_LEVEL")
	v.BindEnv("receiving.timeout", "RECEIVING_TIMEOUT")

	config := &Config{
		MiddlewareAddress: v.GetString("middleware.address"),
		Port:              int32(v.GetInt("port")),
		LogLevel:          v.GetString("log.level"),
		ReceivingTimeout:  v.GetInt("receiving.timeout"),
	}

	return config, nil
}
