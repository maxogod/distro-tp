package client

type Client interface {
	Start(task string) error
	Shutdown()
}
