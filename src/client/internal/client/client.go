package client

import (
	"errors"
	"fmt"
	"io"
	"math/rand"
	"net"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/maxogod/distro-tp/src/client/business/task_executor"
	"github.com/maxogod/distro-tp/src/client/config"
	"github.com/maxogod/distro-tp/src/common/logger"
	"github.com/maxogod/distro-tp/src/common/models/enum"
	"github.com/maxogod/distro-tp/src/common/network"
)

type client struct {
	conf         *config.Config
	conn         network.ConnectionInterface
	taskExecutor task_executor.TaskExecutor
	running      bool
	clientID     string
}

func NewClient(conf *config.Config) (Client, error) {
	conn, err := connectToGateway(conf)
	if err != nil {
		logger.Logger.Debugf("connection error: %v", err)
		return nil, err
	}

	return &client{
		conf:         conf,
		conn:         conn,
		taskExecutor: task_executor.NewTaskExecutor(conf.DataPath, conf.OutputPath, conf.BatchSize, conn, conf),
	}, nil
}

func (c *client) Start(task string) error {
	c.running = true
	c.setupGracefulShutdown()
	defer c.Shutdown()

	// Ensure output directory exists
	if err := os.MkdirAll(c.conf.OutputPath, 0777); err != nil {
		logger.Logger.Errorf("failed to create output directory: %v", err)
		return err
	}

	return c.runTask(task)
}

func (c *client) Shutdown() {
	c.running = false
	err := c.conn.Close()
	if err != nil {
		logger.Logger.Errorf("failed to close Gateway connection: %v", err)
	}
	c.taskExecutor.Close()
	logger.Logger.Infof("action: shutdown | result: success")
}

/* --- UTILS PRIVATE METHODS --- */

func (c *client) getTaskTypeExecutor(task string) (enum.TaskType, func() error, error) {
	switch task {
	case c.conf.Args.T1:
		return enum.T1, c.taskExecutor.Task1, nil
	case c.conf.Args.T2:
		return enum.T2, c.taskExecutor.Task2, nil
	case c.conf.Args.T3:
		return enum.T3, c.taskExecutor.Task3, nil
	case c.conf.Args.T4:
		return enum.T4, c.taskExecutor.Task4, nil
	default:
		return enum.TaskType(0), nil, fmt.Errorf("unknown task: %s", task)
	}
}

func (c *client) runTask(task string) error {
	taskType, taskExecutor, err := c.getTaskTypeExecutor(task)
	if err != nil {
		return err
	}

	if err = c.taskExecutor.SendRequestForTask(taskType, c.clientID); err != nil {
		logger.Logger.Errorf("Error making task request %v", err)
		return err
	}

	clientId, ackErr := c.taskExecutor.AwaitRequestAck(taskType)
	if ackErr != nil {
		logger.Logger.Debugf("Error waiting for ack, reconnecting to another gateway")
		return c.reconnectToGateway(task)
	}

	c.clientID = clientId

	logger.Logger.Infof("Client ID %s starting task %s", c.clientID, task)

	return c.handleTaskError(task, taskExecutor())
}

func (c *client) reconnectToGateway(task string) error {
	conn, err := connectToGateway(c.conf)
	if err != nil {
		logger.Logger.Debugf("connection error: %v", err)
		return err
	}

	c.conn = conn
	c.taskExecutor.Close()
	c.taskExecutor = task_executor.NewTaskExecutor(c.conf.DataPath, c.conf.OutputPath, c.conf.BatchSize, conn, c.conf)

	return c.runTask(task)
}

func (c *client) setupGracefulShutdown() {
	sigChannel := make(chan os.Signal, 1)
	signal.Notify(sigChannel, syscall.SIGTERM, syscall.SIGINT)

	go func() {
		<-sigChannel
		logger.Logger.Infof("action: shutdown_signal | result: received")
		c.Shutdown()
	}()
}

func (c *client) handleTaskError(task string, err error) error {
	if err != nil && !c.running {
		return nil // Errors expected if connection is closed by shutdown mid processing
	}

	if errors.Is(err, syscall.EPIPE) ||
		errors.Is(err, syscall.ECONNRESET) ||
		errors.Is(err, net.ErrClosed) ||
		errors.Is(err, io.EOF) {
		logger.Logger.Debugf("%v. Reconnecting to another gateway", err)
		return c.reconnectToGateway(task)
	}

	logger.Logger.Debugf("%v. Not reconnecting", err)
	return err
}

func connectToGateway(conf *config.Config) (network.ConnectionInterface, error) {
	if conf.MaxNodes <= 0 {
		return nil, fmt.Errorf("gateway count must be positive")
	}

	gatewayIds := make([]int, conf.MaxNodes)
	for i := 0; i < conf.MaxNodes; i++ {
		gatewayIds[i] = i + 1
	}

	rnd := rand.New(rand.NewSource(time.Now().UnixNano()))
	rnd.Shuffle(len(gatewayIds), func(i, j int) { gatewayIds[i], gatewayIds[j] = gatewayIds[j], gatewayIds[i] })

	for attempt := 0; attempt < conf.ConnectionRetries; attempt++ {
		for _, gatewayId := range gatewayIds {
			conn := network.NewConnection()
			serverAddr := fmt.Sprintf("%s%d:%d", conf.ServerHost, gatewayId, conf.ServerPort)
			if err := conn.Connect(serverAddr, conf.ConnectionRetries); err != nil {
				logger.Logger.Debugf("could not connect to gateway%d on attempt %d/%d, trying another", gatewayId, attempt+1, conf.ConnectionRetries)
				continue
			}

			logger.Logger.Infof("connected to gateway%d", gatewayId)

			return conn, nil
		}

		time.Sleep(500 * time.Millisecond)
	}
	return nil, fmt.Errorf("could not connect to any gateway")
}
