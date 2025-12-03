package config

import (
	"fmt"
	"strings"
	"time"

	"github.com/joho/godotenv"
	"github.com/spf13/viper"
)

type HeartbeatConfig struct {
	Host     string
	Port     int
	Interval time.Duration
}

type Config struct {
	ID                         string
	MiddlewareAddress          string
	Port                       int32
	LogLevel                   string
	ReceivingTimeout           int
	MaxClients                 int
	StoragePath                string
	Heartbeat                  HeartbeatConfig
	CompletionAfterDoneTimeout time.Duration
}

func (c Config) String() string {
	return fmt.Sprintf(
		"[CONFIG: Port: %d | LogLevel: %s | ReceivingTimeout: %d | MaxClients: %d]",
		c.Port,
		c.LogLevel,
		c.ReceivingTimeout,
		c.MaxClients,
	)
}

const CONFIG_FILE_PATH = "./config.yaml"

func InitConfig() (*Config, error) {
	v := viper.New()
	v.SetEnvKeyReplacer(strings.NewReplacer(".", "_"))

	_ = godotenv.Load(".env")
	v.AutomaticEnv()

	v.SetDefault("storage.path", "storage")

	v.SetConfigFile(CONFIG_FILE_PATH)
	_ = v.ReadInConfig() // ignore error if file missing

	// Bind env vars to config keys
	v.BindEnv("middleware.address", "MIDDLEWARE_ADDRESS")
	v.BindEnv("port", "PORT")
	v.BindEnv("log.level", "LOG_LEVEL")
	v.BindEnv("receiving.timeout", "RECEIVING_TIMEOUT")
	v.BindEnv("max.clients", "MAX_CLIENTS")
	v.BindEnv("completion_after_done.timeout", "COMPLETION_AFTER_DONE_TIMEOUT")
	v.BindEnv("id", "ID")
	v.BindEnv("storage.path", "STORAGE_PATH")

	heatbeatConf := HeartbeatConfig{
		Host:     v.GetString("heartbeat.host"),
		Port:     v.GetInt("heartbeat.port"),
		Interval: time.Duration(v.GetInt("heartbeat.interval")) * time.Second,
	}

	config := &Config{
		ID:                         v.GetString("id"),
		MiddlewareAddress:          v.GetString("middleware.address"),
		Port:                       int32(v.GetInt("port")),
		LogLevel:                   v.GetString("log.level"),
		ReceivingTimeout:           v.GetInt("receiving.timeout"),
		MaxClients:                 v.GetInt("max.clients"),
		StoragePath:                v.GetString("storage.path"),
		Heartbeat:                  heatbeatConf,
		CompletionAfterDoneTimeout: time.Duration(v.GetInt("completion_after_done.timeout")) * time.Second,
	}

	return config, nil
}
