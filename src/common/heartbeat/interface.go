package heartbeat

// HeartBeatHandler is responsible for sending heartbeat signals to
// a specified host and port at regular intervals.
type HeartBeatHandler interface {

	// StartSending initiates the heartbeat sending process in a routine.
	StartSending() error

	// StartReceiving initiates the heartbeat receiving process in a routine.
	StartReceiving(onTimeoutFunc func(), timeoutAmount int) error

	// Stop halts the heartbeat sending or receiving process.
	Stop()

	// Close stops the heartbeat sending process and cleans up resources.
	Close()
}
