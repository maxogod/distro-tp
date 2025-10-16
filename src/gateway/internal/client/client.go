package client

import (
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/maxogod/distro-tp/src/common/logger"
	"github.com/maxogod/distro-tp/src/common/network"
	"github.com/maxogod/distro-tp/src/gateway/business/task_executor"
	"github.com/maxogod/distro-tp/src/gateway/config"
)

var log = logger.GetLogger()

type client struct {
	conf         *config.Config
	conn         network.ConnectionInterface
	tastExecutor task_executor.TaskExecutor
	running      bool
}

func NewClient(conf *config.Config) (Client, error) {
	// Connect to server
	conn := network.NewConnection()
	err := conn.Connect(fmt.Sprintf("%s:%d", conf.ServerHost, conf.ServerPort), conf.ConnectionRetries)
	if err != nil {
		log.Errorf("could not connect to server: %v", err)
		return nil, err
	}

	return &client{
		conf:         conf,
		conn:         conn,
		tastExecutor: task_executor.NewTaskExecutor(conf.DataPath, conf.OutputPath, conf.BatchSize, conn),
	}, nil
}

func (c *client) Start(task string) error {
	c.setupGracefulShutdown()
	defer c.Shutdown()

	// Ensure output directory exists
	if err := os.MkdirAll(c.conf.OutputPath, 0755); err != nil {
		log.Errorf("failed to create output directory: %v", err)
		return err
	}

	switch task {
	case ARG_T1:
		return c.handleTaskError(c.tastExecutor.Task1())
	case ARG_T2:
		return c.handleTaskError(c.tastExecutor.Task2())
	case ARG_T3:
		return c.handleTaskError(c.tastExecutor.Task3())
	case ARG_T4:
		return c.handleTaskError(c.tastExecutor.Task4())
	}

	return fmt.Errorf("unknown task: %s", task)
}

func (c *client) Shutdown() {
	c.running = false
	c.conn.Close()
	c.tastExecutor.Close()
	log.Infof("action: shutdown | result: success")
}

/* --- UTILS PRIVATE METHODS --- */

func (c *client) setupGracefulShutdown() {
	sigChannel := make(chan os.Signal, 1)
	signal.Notify(sigChannel, syscall.SIGTERM, syscall.SIGINT)

	go func() {
		<-sigChannel
		log.Infof("action: shutdown_signal | result: received")
		c.Shutdown()
	}()
}

func (c *client) handleTaskError(err error) error {
	if err != nil && !c.running {
		return nil // Errors expected if connection is closed by shutdown mid processing
	}
	return err
}
