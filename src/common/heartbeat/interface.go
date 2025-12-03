package heartbeat

// HeartBeatHandler is responsible for sending heartbeat signals to
// a specified host and port at regular intervals.
type HeartBeatHandler interface {

	// StartSending initiates the heartbeat sending process in a routine.
	StartSending() error

	// Close finishes the heartbeat sending process and cleans up resources.
	Close()
}
