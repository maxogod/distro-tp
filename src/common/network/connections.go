package network

import (
	"encoding/binary"
	"io"
	"net"
	"time"
)

const HEADER_SIZE = 4
const WAIT_INTERVAL = 1 * time.Second

type connectionInterface struct {
	conn net.Conn
}

func NewConnection() ConnectionInterface {
	return &connectionInterface{}
}

func NewConnectionFromExistent(conn net.Conn) ConnectionInterface {
	return &connectionInterface{conn: conn}
}

func (c *connectionInterface) Connect(serverAddr string, retries int) error {
	var conn net.Conn
	var err error
	for range retries {
		conn, err = net.Dial("tcp", serverAddr)
		if err == nil {
			break
		}
		time.Sleep(WAIT_INTERVAL)
	}
	if err != nil {
		return err
	}
	c.conn = conn
	return nil
}

func (c *connectionInterface) IsConnected() bool {
	return c.conn != nil
}

func (c *connectionInterface) ReceiveData() ([]byte, error) {
	if !c.IsConnected() {
		return nil, io.EOF
	}
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

func (c *connectionInterface) SendData(data []byte) error {
	if !c.IsConnected() {
		return io.EOF
	}
	lenBytes := make([]byte, HEADER_SIZE)
	binary.BigEndian.PutUint32(lenBytes, uint32(len(data)))
	payload := append(lenBytes, data...)

	// Send the length header with data payload
	return c.writeFull(payload)
}

func (c *connectionInterface) Close() error {
	if c.conn != nil {
		err := c.conn.Close()
		if err != nil {
			return err
		}
		c.conn = nil
	}
	return nil
}

// --- PRIVATE METHODS ---

func (c *connectionInterface) readFull(buf []byte) error {
	totalRead := 0
	for totalRead < len(buf) {
		n, err := c.conn.Read(buf[totalRead:])
		if err != nil {
			if err == io.EOF {
				c.conn = nil
			}
			return err
		}
		totalRead += n
	}
	return nil
}

func (c *connectionInterface) writeFull(buf []byte) error {
	totalWritten := 0
	for totalWritten < len(buf) {
		n, err := c.conn.Write(buf[totalWritten:])
		if err != nil {
			if err == io.EOF {
				c.conn = nil
			}
			return err
		}
		totalWritten += n
	}
	return nil
}
