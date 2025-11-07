package logger

import (
	"go.uber.org/zap"
)

type LoggerEnvironment string

const (
	LoggerEnvDevelopment LoggerEnvironment = "development"
	LoggerEnvProduction  LoggerEnvironment = "production"
)

var Logger *zap.SugaredLogger

func InitLogger(environ LoggerEnvironment) {
	if Logger == nil {
		switch environ {
		case LoggerEnvDevelopment:
			logger, _ := zap.NewDevelopment()
			Logger = logger.Sugar()
		case LoggerEnvProduction:
			logger, _ := zap.NewProduction()
			Logger = logger.Sugar()
		}
	}
}

func Sync() {
	if Logger != nil {
		Logger.Sync()
	}
}
