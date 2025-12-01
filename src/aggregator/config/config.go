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
	ID             int
	Host           string
	Port           int
	MaxNodes       int
	ConnectedNodes []string
}

func (lec LeaderElectionConfig) String() string {

	connectedNodesStr := strings.Join(lec.ConnectedNodes, ", ")

	return fmt.Sprintf(" ID: %d | ConnectedNodes: [%s], Amount: %d | Host: %s | Port: %d", lec.ID, connectedNodesStr, len(lec.ConnectedNodes), lec.Host, lec.Port)
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
		" %s | Middleware Address: %s | LogLevel: %s | Limits: [TransactionSendLimit=%d, MaxAmountToSend=%d]",
		c.LeaderElection.String(),
		c.Address,
		c.LogLevel,
		c.Limits.TransactionSendLimit,
		c.Limits.MaxAmountToSend,
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

	heatbeatConf := HeartbeatConfig{
		Host:     v.GetString("heartbeat.host"),
		Port:     v.GetInt("heartbeat.port"),
		Interval: time.Duration(v.GetInt("heartbeat.interval")) * time.Second,
	}

	leaderElectionConf := LeaderElectionConfig{
		ID:             v.GetInt("leader_election.id"),
		MaxNodes:       v.GetInt("leader_election.maxNodes"),
		ConnectedNodes: ViperGetStringSliceWithDefault(v, "leader_election.nodes", []string{}),
		Host:           v.GetString("leader_election.host"),
		Port:           v.GetInt("leader_election.port"),
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

func ViperGetStringSliceWithDefault(v *viper.Viper, key string, defaultValue []string) []string {
	if !v.IsSet(key) {
		return defaultValue
	}
	str := v.GetString(key)
	if str != "" {
		values := strings.Split(str, ",")
		for i, v := range values {
			values[i] = strings.TrimSpace(v)
		}
		return values
	}
	return defaultValue
}
