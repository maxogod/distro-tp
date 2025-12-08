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
	Amount   int
	Interval time.Duration
}

type Config struct {
	MiddlewareAddress  string
	Port               int32
	HealthCheckPort    int
	LogLevel           string
	Heartbeat          HeartbeatConfig
	MaxControllerNodes int
}

func (c Config) String() string {
	return fmt.Sprintf(
		"[CONFIG: Port: %d | LogLevel: %s]",
		c.Port,
		c.LogLevel,
	)
}

const CONFIG_FILE_PATH = "./config.yaml"

func InitConfig() (*Config, error) {
	return initConfig(CONFIG_FILE_PATH)
}

func InitConfigWithPath(configFilePath string) (*Config, error) {
	path := CONFIG_FILE_PATH
	if configFilePath != "" {
		path = configFilePath
	}

	return initConfig(path)
}

func initConfig(configFilePath string) (*Config, error) {
	v := viper.New()
	v.SetEnvKeyReplacer(strings.NewReplacer(".", "_"))

	_ = godotenv.Load(".env")
	v.AutomaticEnv()

	v.SetConfigFile(configFilePath)
	_ = v.ReadInConfig() // ignore error if file missing

	// Bind env vars to config keys
	v.BindEnv("gateway.address", "GATEWAY_ADDRESS")
	v.BindEnv("port", "PORT")
	v.BindEnv("healthcheck.port", "HEALTHCHECK_PORT")
	v.BindEnv("log.level", "LOG_LEVEL")
	v.BindEnv("max_controller_nodes", "MAX_CONTROLLER_NODES")
	v.BindEnv("heartbeat.amount", "EOL_NODES")

	heatbeatConf := HeartbeatConfig{
		Host:     v.GetString("heartbeat.host"),
		Port:     v.GetInt("heartbeat.port"),
		Amount:   v.GetInt("heartbeat.amount"),
		Interval: time.Duration(v.GetInt("heartbeat.interval")) * time.Millisecond,
	}

	config := &Config{
		MiddlewareAddress:  v.GetString("middleware.address"),
		Port:               int32(v.GetInt("port")),
		HealthCheckPort:    v.GetInt("healthcheck.port"),
		LogLevel:           v.GetString("log.level"),
		Heartbeat:          heatbeatConf,
		MaxControllerNodes: v.GetInt("max_controller_nodes"),
	}

	return config, nil
}
