package client

import (
	"fmt"
	"os"
	"os/signal"
	"strconv"
	"syscall"

	"github.com/google/uuid"
	"github.com/maxogod/distro-tp/src/client/business/task_executor"
	"github.com/maxogod/distro-tp/src/client/config"
	"github.com/maxogod/distro-tp/src/common/logger"
	"github.com/maxogod/distro-tp/src/common/network"
)

type client struct {
	conf         *config.Config
	conn         network.ConnectionInterface
	tastExecutor task_executor.TaskExecutor
	running      bool
	clientID     string
}

func NewClient(conf *config.Config) (Client, error) {
	clientID := uuid.New().String()
	connections, err := connectToGateways(conf, clientID)
	if err != nil {
		logger.Logger.Errorf("could not connect to gateways: %v", err)
		return nil, err
	}

	conn, err := awaitLeaderGateway(connections)
	if err != nil {
		logger.Logger.Errorf("could not determine leader gateway: %v", err)
		return nil, err
	}

	return &client{
		conf:         conf,
		conn:         conn,
		clientID:     clientID,
		tastExecutor: task_executor.NewTaskExecutor(conf.DataPath, conf.OutputPath, conf.BatchSize, conn, conf),
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

	switch task {
	case c.conf.Args.T1:
		return c.handleTaskError(c.tastExecutor.Task1())
	case c.conf.Args.T2:
		return c.handleTaskError(c.tastExecutor.Task2())
	case c.conf.Args.T3:
		return c.handleTaskError(c.tastExecutor.Task3())
	case c.conf.Args.T4:
		return c.handleTaskError(c.tastExecutor.Task4())
	}

	return fmt.Errorf("unknown task: %s", task)
}

func (c *client) Shutdown() {
	c.running = false
	// TODO: Cerrar todas las conexiones
	c.conn.Close()
	c.tastExecutor.Close()
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

func connectToGateways(conf *config.Config, clientID string) (map[int]network.ConnectionInterface, error) {
	if conf.MaxNodes <= 0 {
		return nil, fmt.Errorf("gateway count must be positive")
	}

	connections := make(map[int]network.ConnectionInterface, conf.MaxNodes)
	for i := 1; i <= conf.MaxNodes; i++ {
		conn := network.NewConnection()
		serverAddr := fmt.Sprintf("%s%d:%d", conf.ServerHost, i, conf.ServerPort)
		if err := conn.Connect(serverAddr, conf.ConnectionRetries); err != nil {
			logger.Logger.Debugf("could not connect to gateway %s: %v", serverAddr, err)
			continue
		}

		connections[i] = conn

		// TODO: Mandar un DataEnvelope en vez del ID directamente
		if err := conn.SendData([]byte(clientID)); err != nil {
			return nil, fmt.Errorf("failed to send client ID: %w", err)
		}
	}

	if len(connections) == 0 {
		return nil, fmt.Errorf("could not connect to any gateway")
	}

	return connections, nil
}

func awaitLeaderGateway(conns map[int]network.ConnectionInterface) (network.ConnectionInterface, error) {
	respCh := make(chan int, len(conns))

	for idx, conn := range conns {
		go func(index int, connection network.ConnectionInterface) {
			idBytes, err := connection.ReceiveData()
			if err != nil {
				logger.Logger.Errorf("connection with gateway%d closed", index)
				return
			}

			id := string(idBytes)
			logger.Logger.Infof("ReadyForData received from gateway%s", id)

			idInt, err := strconv.Atoi(id)
			if err != nil {
				logger.Logger.Errorf("invalid gateway id received from gateway%d: %v", index, err)
				return
			}

			respCh <- idInt
		}(idx, conn)
	}

	leaderIndex := -1
	for {
		idx := <-respCh
		if idx > 0 {
			leaderIndex = idx
			break
		}
	}

	logger.Logger.Infof("action: gateway_leader_found | gateway%d", leaderIndex)

	conn, ok := conns[leaderIndex]
	if !ok {
		return nil, fmt.Errorf("leader gateway %d not found in connections", leaderIndex)
	}
	return conn, nil
}
