package config

import (
	"fmt"
	"strings"
	"time"

	"github.com/pkg/errors"
	"github.com/spf13/viper"
)

type HeartbeatConfig struct {
	Host     string
	Port     int
	Interval time.Duration
}

type Limits struct {
	TransactionSendLimit int32
	MaxAmountToSend      int
}

type LeaderElectionConfig struct {
	ID       int
	MaxNodes int
	Host     string
	Port     int
}

type Config struct {
	Address        string
	LogLevel       string
	Limits         Limits
	Heartbeat      HeartbeatConfig
	LeaderElection LeaderElectionConfig
}

func (c Config) String() string {
	return fmt.Sprintf(
		"Address: %s | LogLevel: %s | Limits: [TransactionSendLimit=%d, MaxAmountToSend=%d]",
		c.Address,
		c.LogLevel,
		c.Limits.TransactionSendLimit,
		c.Limits.MaxAmountToSend,
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

	heatbeatConf := HeartbeatConfig{
		Host:     v.GetString("heartbeat.host"),
		Port:     v.GetInt("heartbeat.port"),
		Interval: time.Duration(v.GetInt("heartbeat.interval")) * time.Second,
	}

	leaderElectionConf := LeaderElectionConfig{
		ID:       v.GetInt("leader_election.id"),
		MaxNodes: v.GetInt("leader_election.maxNodes"),
		Host:     v.GetString("leader_election.host"),
		Port:     v.GetInt("leader_election.port"),
	}

	limits := Limits{
		TransactionSendLimit: v.GetInt32("limits.transaction_send_limit"),
		MaxAmountToSend:      v.GetInt("limits.max_amount_to_send"),
	}

	config := &Config{
		Address:        v.GetString("gateway.address"),
		LogLevel:       v.GetString("log.level"),
		Limits:         limits,
		Heartbeat:      heatbeatConf,
		LeaderElection: leaderElectionConf,
	}

	return config, nil
}
