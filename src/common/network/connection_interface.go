package network

type ConnectionInterface interface {
	// Connect initiates a connection to the specified server address.
	Connect(serverAddr string, retries int) error

	// IsConnected checks if the connection is currently established.
	IsConnected() bool

	// ReceiveData reads data fully from the connection.
	ReceiveData() ([]byte, error)

	// SendData writes data fully to the connection.
	SendData(data []byte) error

	// Close terminates the connection.
	Close() error
}
