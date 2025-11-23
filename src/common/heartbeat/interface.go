package heartbeat

// HeartBeatHandler is responsible for sending heartbeat signals to
// a specified host and port at regular intervals.
type HeartBeatHandler interface {

	// StartSending initiates the heartbeat sending process in a routine.
	StartSending() error

	// StartSendingToAll initiates the heartbeat sending process to multiple hosts.
	StartSendingToAll(destinationAddrs []string, connectionRetries int) error

	// StartReceiving initiates the heartbeat receiving process in a routine.
	// The amount of heartbeats received is passed to the onTimeoutFunc when a timeout occurs.
	// This is used for testing purposes to see how many heartbeats were received before timing out.
	StartReceiving(onTimeoutFunc func(amountOfHeartbeats int), timeoutAmount int) error

	// ChangeAddress updates the destination host and port for sending heartbeats.
	// This also halts any ongoing sending / receiving process.
	ChangeAddress(host string, port int)

	// Close finishes the heartbeat sending process and cleans up resources.
	Close()

	// Stop halts the heartbeat sending or receiving process, allowing for a potential restart.
	Stop()
}
