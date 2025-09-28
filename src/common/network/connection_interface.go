package network

import (
	"io"
	"net"
)

type ConnectionInterface struct {
	conn net.Conn
}

func NewConnectionInterface() *ConnectionInterface {
	return &ConnectionInterface{}
}

func ConnectedClientInterface(conn net.Conn) *ConnectionInterface {
	return &ConnectionInterface{conn: conn}
}

func (c *ConnectionInterface) Connect(serverAddr string) error {
	conn, err := net.Dial("tcp", serverAddr)
	if err != nil {
		return err
	}
	c.conn = conn
	return nil
}

func (c *ConnectionInterface) ReceiveData(buffer []byte) error {

	_, err := io.ReadFull(c.conn, buffer)
	if err != nil {
		return err
	}
	return nil
}

func (c *ConnectionInterface) SendData(data []byte) error {
	totalWritten := 0
	for totalWritten < len(data) {
		n, err := c.conn.Write(data[totalWritten:])
		if err != nil {
			return err
		}
		totalWritten += n
	}
	return nil
}

func (c *ConnectionInterface) Close() error {
	if c.conn != nil {
		err := c.conn.Close()
		if err != nil {
			return err
		}
	}
	return nil
}
