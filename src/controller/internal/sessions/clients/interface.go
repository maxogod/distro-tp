package clients

// ClientSession represents a client and its operations.
type ClientSession interface {
	// InitiateControlSequence starts the EOF sequence for the given client ID.
	// Messages will be counted per stage and once done, execute EOF for joiner and aggregator.
	InitiateControlSequence() error

	// IsFinished checks if the client session has finished processing.
	IsFinished() bool

	// Close gracefully shuts down the client session, releasing any resources attached to the connection.
	Close()

	// SendControllerReady notifies the gateway that the controller is ready to receive messages
	SendControllerReady()
}
