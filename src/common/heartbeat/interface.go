package heartbeat

// HeartBeatSender is responsible for sending heartbeat signals to
// a specified host and port at regular intervals.
type HeartBeatSender interface {

	// Start initiates the heartbeat sending process in a routine.
	Start() error

	// Close stops the heartbeat sending process and cleans up resources.
	Close()
}
