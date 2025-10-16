package network

import (
	"fmt"
	"net"

	"github.com/maxogod/distro-tp/src/common/network"
)

type connectionManager struct {
	port     int32
	listener net.Listener
}

func NewConnectionManager(port int32) ConnectionManager {
	return &connectionManager{
		port: port,
	}
}

func (cm *connectionManager) StartListening() error {
	ln, err := net.Listen("tcp", fmt.Sprintf(":%d", cm.port))
	if err != nil {
		return err
	}

	cm.listener = ln

	return nil
}

func (cm *connectionManager) AcceptConnection() (network.ConnectionInterface, error) {
	conn, err := cm.listener.Accept()
	if err != nil {
		return nil, err
	}
	return network.NewConnectionFromExistent(conn), nil
}

func (cm *connectionManager) Close() error {
	if cm.listener != nil {
		err := cm.listener.Close()
		if err != nil {
			return err
		}
	}
	return nil
}
