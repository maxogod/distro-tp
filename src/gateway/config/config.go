package config

import (
	"fmt"
	"strings"

	"github.com/joho/godotenv"
	"github.com/spf13/viper"
)

type HeartbeatConfig struct {
	Host     string
	Port     int
	Interval int
}

type Config struct {
	MiddlewareAddress string
	Port              int32
	HealthCheckPort   int
	LogLevel          string
	ReceivingTimeout  int
	Heartbeat         HeartbeatConfig
}

func (c Config) String() string {
	return fmt.Sprintf(
		"[CONFIG: Port: %d | LogLevel: %s | ReceivingTimeout: %d]",
		c.Port,
		c.LogLevel,
		c.ReceivingTimeout,
	)
}

const CONFIG_FILE_PATH = "./config.yaml"

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
	v.BindEnv("receiving.timeout", "RECEIVING_TIMEOUT")

	heatbeatConf := HeartbeatConfig{
		Host:     v.GetString("heartbeat.host"),
		Port:     v.GetInt("heartbeat.port"),
		Interval: v.GetInt("heartbeat.interval"),
	}

	config := &Config{
		MiddlewareAddress: v.GetString("middleware.address"),
		Port:              int32(v.GetInt("port")),
		HealthCheckPort:   v.GetInt("healthcheck.port"),
		LogLevel:          v.GetString("log.level"),
		ReceivingTimeout:  v.GetInt("receiving.timeout"),
		Heartbeat:         heatbeatConf,
	}

	return config, nil
}
