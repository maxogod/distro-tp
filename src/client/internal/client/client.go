package client

import (
	"fmt"
	"math/rand"
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
		logger.Logger.Errorf("could not connect to gateways: %v", err)
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
	if err := os.MkdirAll(c.conf.OutputPath, 0755); err != nil {
		logger.Logger.Errorf("failed to create output directory: %v", err)
		return err
	}

	var taskType enum.TaskType
	var taskExecutor func() error

	switch task {
	case c.conf.Args.T1:
		taskType = enum.T1
		taskExecutor = c.taskExecutor.Task1
	case c.conf.Args.T2:
		taskType = enum.T2
		taskExecutor = c.taskExecutor.Task2
	case c.conf.Args.T3:
		taskType = enum.T3
		taskExecutor = c.taskExecutor.Task3
	case c.conf.Args.T4:
		taskType = enum.T4
		taskExecutor = c.taskExecutor.Task4
	default:
		return fmt.Errorf("unknown task: %s", task)
	}

	if err := c.taskExecutor.SendRequestForTask(taskType); err != nil {
		logger.Logger.Errorf("Error making task request %v", err)
	}

	clientId, ackErr := c.taskExecutor.AwaitRequestAck(taskType)
	if ackErr != nil {
		return ackErr
	}

	c.clientID = clientId

	logger.Logger.Infof("Client ID %s starting task %s", c.clientID, task)

	return c.handleTaskError(taskExecutor())
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

func (c *client) setupGracefulShutdown() {
	sigChannel := make(chan os.Signal, 1)
	signal.Notify(sigChannel, syscall.SIGTERM, syscall.SIGINT)

	go func() {
		<-sigChannel
		logger.Logger.Infof("action: shutdown_signal | result: received")
		c.Shutdown()
	}()
}

func (c *client) handleTaskError(err error) error {
	if err != nil && !c.running {
		return nil // Errors expected if connection is closed by shutdown mid processing
	}
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

	for _, gatewayId := range gatewayIds {
		conn := network.NewConnection()
		serverAddr := fmt.Sprintf("%s%d:%d", conf.ServerHost, gatewayId, conf.ServerPort)
		if err := conn.Connect(serverAddr, conf.ConnectionRetries); err != nil {
			logger.Logger.Debugf("could not connect to gateway%d, trying with another", gatewayId)
			continue
		}

		logger.Logger.Infof("connected to gateway%d", gatewayId)

		return conn, nil
	}

	return nil, fmt.Errorf("could not connect to any gateway")
}
