package client

import (
	"fmt"
	"os"

	"github.com/maxogod/distro-tp/src/common/logger"
	"github.com/maxogod/distro-tp/src/common/network"
	"github.com/maxogod/distro-tp/src/gateway/business/task_executor"
	"github.com/maxogod/distro-tp/src/gateway/config"
)

var log = logger.GetLogger()

type client struct {
	conf *config.Config
}

func NewClient(conf *config.Config) Client {
	return &client{
		conf: conf,
	}
}

func (c *client) Start(task string) error {
	// Connect to server
	conn := network.NewConnection()
	err := conn.Connect(fmt.Sprintf("%s:%d", c.conf.ServerHost, c.conf.ServerPort), c.conf.ConnectionRetries)
	if err != nil {
		log.Errorf("could not connect to server: %v", err)
		return err
	}
	defer conn.Close()

	// Ensure output directory exists
	if err := os.MkdirAll(c.conf.OutputPath, 0755); err != nil {
		log.Errorf("failed to create output directory: %v", err)
		return err
	}

	// Request processing of a task
	exec := task_executor.NewTaskExecutor(c.conf.DataPath, c.conf.OutputPath, c.conf.BatchSize, conn)

	switch task {
	case ARG_T1:
		return exec.Task1()
	case ARG_T2:
		return exec.Task2()
	case ARG_T3:
		return exec.Task3()
	case ARG_T4:
		return exec.Task4()
	}

	log.Errorf("unknown task: %s", task)
	return fmt.Errorf("unknown task: %s", task)
}
