package healthcheck

import "context"

// PingServer is a util server that runs alongside other server
// for the sole purpose of checking if process is ready to accept connections.
type PingServer interface {

	// Run starts the ping server on the given address.
	Run()

	// Close closes the ping server.
	Shutdown(ctx context.Context)
}
