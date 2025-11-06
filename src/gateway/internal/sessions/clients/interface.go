package clients

// ClientSession represents a client and its operations.
type ClientSession interface {
	// IsFinished checks if the client session has completed its tasks.
	IsFinished() bool

	// ProcessRequest handles incoming requests for the client session, sends the data to be processed
	// and waits for the response. It returns an error if processing fails.
	ProcessRequest() error

	// Close gracefully shuts down the client session, releasing any resources attached to the connection.
	Close()
}
