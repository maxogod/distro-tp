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
	HealthCheckPort   int
	LogLevel          string
}

func (c Config) String() string {
	return fmt.Sprintf(
		"[CONFIG: Port: %d | LogLevel: %s]",
		c.Port,
		c.LogLevel,
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
	v.BindEnv("gateway.address", "GATEWAY_ADDRESS")
	v.BindEnv("port", "PORT")
	v.BindEnv("healthcheck.port", "HEALTHCHECK_PORT")
	v.BindEnv("log.level", "LOG_LEVEL")

	config := &Config{
		MiddlewareAddress: v.GetString("middleware.address"),
		Port:              int32(v.GetInt("port")),
		HealthCheckPort:   v.GetInt("healthcheck.port"),
		LogLevel:          v.GetString("log.level"),
	}

	return config, nil
}
