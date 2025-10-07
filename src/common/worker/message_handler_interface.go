package worker

type MessageHandler interface {
	Start() error
	Close() error
}
