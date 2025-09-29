package network

import (
	"encoding/binary"
	"net"
)

const HEADER_SIZE = 4

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

func (c *ConnectionInterface) ReceiveData() ([]byte, error) {
	// Read the length of the incoming message
	header := make([]byte, HEADER_SIZE)
	if err := c.readFull(header); err != nil {
		return nil, err
	}
	data := make([]byte, binary.BigEndian.Uint32(header))

	if err := c.readFull(data); err != nil {
		return nil, err
	}

	return data, nil
}

func (c *ConnectionInterface) SendData(data []byte) error {
	lenBytes := make([]byte, HEADER_SIZE)
	binary.BigEndian.PutUint32(lenBytes, uint32(len(data)))
	payload := append(lenBytes, data...)

	// Send the length header with data payload
	return c.writeFull(payload)
}

func (c *ConnectionInterface) readFull(buf []byte) error {
	totalRead := 0
	for totalRead < len(buf) {
		n, err := c.conn.Read(buf[totalRead:])
		if err != nil {
			return err
		}
		totalRead += n
	}
	return nil
}

// Private helper to write all bytes, handling short writes
func (c *ConnectionInterface) writeFull(buf []byte) error {
	totalWritten := 0
	for totalWritten < len(buf) {
		n, err := c.conn.Write(buf[totalWritten:])
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
