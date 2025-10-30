package config

import (
	"errors"
	"fmt"
	"strings"

	"github.com/spf13/viper"
)

type Paths struct {
	Transactions     string
	TransactionItems string
	MenuItems        string
	Stores           string
	Users            string
}

type OutputFiles struct {
	T1   string
	T2_1 string
	T2_2 string
	T3   string
	T4   string
}

type Args struct {
	T1 string
	T2 string
	T3 string
	T4 string
}

type Config struct {
	ServerHost        string
	ServerPort        int
	ConnectionRetries int
	DataPath          string
	OutputPath        string
	BatchSize         int
	Paths             Paths
	OutputFiles       OutputFiles
	Args              Args
}

func (c *Config) String() string {
	return fmt.Sprintf("ServerHost: %s, ServerPort: %d, DataPath: %s", c.ServerHost, c.ServerPort, c.DataPath)
}

const CONFIG_FILE_PATH = "./config.yaml"

func InitConfig() (*Config, error) {
	v := viper.New()
	v.SetEnvKeyReplacer(strings.NewReplacer(".", "_"))
	v.AutomaticEnv()

	v.SetConfigFile(CONFIG_FILE_PATH)
	if err := v.ReadInConfig(); err != nil {
		return nil, errors.New("failed to read config file " + CONFIG_FILE_PATH)
	}

	config := &Config{
		ServerHost:        v.GetString("server.host"),
		ServerPort:        v.GetInt("server.port"),
		ConnectionRetries: v.GetInt("server.connection_retries"),
		DataPath:          v.GetString("data_path"),
		OutputPath:        v.GetString("output_path"),
		BatchSize:         v.GetInt("batch_size"),
		Paths: Paths{
			Transactions:     v.GetString("paths.transactions"),
			TransactionItems: v.GetString("paths.transaction_items"),
			MenuItems:        v.GetString("paths.menu_items"),
			Stores:           v.GetString("paths.stores"),
			Users:            v.GetString("paths.users"),
		},
		OutputFiles: OutputFiles{
			T1:   v.GetString("output_files.t1"),
			T2_1: v.GetString("output_files.t2_1"),
			T2_2: v.GetString("output_files.t2_2"),
			T3:   v.GetString("output_files.t3"),
			T4:   v.GetString("output_files.t4"),
		},
		Args: Args{
			T1: v.GetString("args.t1"),
			T2: v.GetString("args.t2"),
			T3: v.GetString("args.t3"),
			T4: v.GetString("args.t4"),
		},
	}

	fmt.Printf("Config loaded: %s\n", config.String())

	return config, nil
}
