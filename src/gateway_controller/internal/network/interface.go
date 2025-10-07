package network

import "github.com/maxogod/distro-tp/src/common/network"

// ConnectionManager defines the interface for managing incoming network connections and clients.
type ConnectionManager interface {

	// StartListening starts the connection manager to listen for incoming connections.
	// It returns an error if it fails to start listening on the given port at initialization.
	StartListening() error

	// AcceptConnection waits for and accepts an incoming connection in a blocking manner.
	AcceptConnection() (network.ConnectionInterface, error)

	// Close closes the connection manager and releases any resources attatched to the listening port.
	Close() error
}
