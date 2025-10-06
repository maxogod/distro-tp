package worker

type MessageHandler interface {
	Start() error
	FinishClient(clientID string) error
	Close() error
}
