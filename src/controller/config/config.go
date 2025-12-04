package config

import (
	"fmt"
	"strconv"
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
	ID                 string
	NumericID          int32
	MiddlewareAddress  string
	Port               int32
	LogLevel           string
	MaxClients         int
	MaxUnackedCounters int
	StoragePath        string
	Heartbeat          HeartbeatConfig
}

func (c Config) String() string {
	return fmt.Sprintf(
		"[CONFIG: Port: %d | LogLevel: %s | MaxClients: %d]",
		c.Port,
		c.LogLevel,
		c.MaxClients,
	)
}

const CONFIG_FILE_PATH = "./config.yaml"
const prefix = "controller"

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
	v.BindEnv("max.clients", "MAX_CLIENTS")
	v.BindEnv("id", "ID")
	v.BindEnv("storage.path", "STORAGE_PATH")
	v.BindEnv("max.unacked_counters", "MAX_UNACKED_COUNTERS")

	heatbeatConf := HeartbeatConfig{
		Host:     v.GetString("heartbeat.host"),
		Port:     v.GetInt("heartbeat.port"),
		Interval: time.Duration(v.GetInt("heartbeat.interval")) * time.Millisecond,
	}

	numericID, err := parseControllerID(v.GetString("id"))
	if err != nil {
		return nil, err
	}

	config := &Config{
		ID:                 v.GetString("id"),
		NumericID:          numericID,
		MiddlewareAddress:  v.GetString("middleware.address"),
		Port:               int32(v.GetInt("port")),
		LogLevel:           v.GetString("log.level"),
		MaxClients:         v.GetInt("max.clients"),
		MaxUnackedCounters: v.GetInt("max.unacked_counters"),
		StoragePath:        v.GetString("storage.path"),
		Heartbeat:          heatbeatConf,
	}

	return config, nil
}

func parseControllerID(id string) (int32, error) {
	numStr := strings.TrimPrefix(id, prefix)
	if numStr == "" {
		return 0, fmt.Errorf("controller id %s has no numeric suffix", id)
	}
	n, err := strconv.Atoi(numStr)
	if err != nil {
		return 0, err
	}
	return int32(n), nil
}
