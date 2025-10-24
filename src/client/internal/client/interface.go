package client

const (
	ARG_T1 = "t1"
	ARG_T2 = "t2"
	ARG_T3 = "t3"
	ARG_T4 = "t4"
)

type Client interface {
	Start(task string) error
	Shutdown()
}
