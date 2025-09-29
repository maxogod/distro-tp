package config

import (
	"fmt"

	"github.com/joho/godotenv"
	"github.com/spf13/viper"
)

type Config struct {
	Port     int32
	LogLevel string
}

func (c Config) String() string {
	return fmt.Sprintf(
		"[CONFIG: Port: %d | LogLevel: %s]",
		c.Port,
		c.LogLevel,
	)
}

func InitConfig() (*Config, error) {

	v := viper.New()

	// this function will do nothing if the file is missing,
	// so only environment variables will be used.
	_ = godotenv.Load(".env")
	v.AutomaticEnv()

	config := &Config{
		Port:     v.GetInt32("PORT"),
		LogLevel: v.GetString("LOG_LEVEL"),
	}

	return config, nil
}
