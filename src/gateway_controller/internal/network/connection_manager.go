package network

import (
	"fmt"
	"net"

	"github.com/maxogod/distro-tp/src/common/network"
)

type ConnectionManager struct {
	port     int32
	listener net.Listener
}

// TODO: implement connection manager methods to server logic
func NewConnectionManager(port int32) *ConnectionManager {
	return &ConnectionManager{
		port: port,
	}
}

func (cm *ConnectionManager) StartListening() error {
	ln, err := net.Listen("tcp", fmt.Sprintf(":%d", cm.port))
	if err != nil {
		return err
	}
	defer ln.Close()

	return nil
}

func (cm *ConnectionManager) AcceptConnection() (*network.ConnectionInterface, error) {
	conn, err := cm.listener.Accept()
	if err != nil {
		return nil, err
	}
	return network.ConnectedClientInterface(conn), nil
}

func (cm *ConnectionManager) Close() error {
	if cm.listener != nil {
		err := cm.listener.Close()
		if err != nil {
			return err
		}
	}
	return nil
}
