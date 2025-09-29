package client

import (
	"fmt"

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
	log.Infoln("started")

	conn := network.NewConnectionInterface()
	conn.Connect(fmt.Sprintf("%s:%d", c.conf.ServerHost, c.conf.ServerPort))
	defer conn.Close()

	exec := task_executor.NewTaskExecutor(c.conf.DataPath, conn)

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
