package task_executor

import (
	"github.com/maxogod/distro-tp/src/common/models/enum"
)

// TaskExecutor handles the execution of tasks by communicating with the server
// and fetching the processed data accordingly.
type TaskExecutor interface {
	// Task1 sends transactions to the server and waits for the response containing
	// the filtered transactions and writes them to the given output file.
	Task1() error

	// Task2  sends transaction items and menu items to the server and waits for the response containing
	// the top 1 most sold and top 1 most profit and writes them to the given output file.
	Task2() error

	// Task3 sends transactions and stores to the server and waits for the response containing
	// the TPV per store per semester and writes them to the given output file.
	Task3() error

	// Task4 sends transactions, users and stores to the server and waits for the response containing
	// the top 3 users with more transactions and writes them to the given output file.
	Task4() error

	// Close releases any resources held by the TaskExecutor.
	Close()

	// SendRequestForTask sends a request for a given task type to the server.
	SendRequestForTask(taskType enum.TaskType) error

	// AwaitRequestAck waits for the server to acknowledge the request for a given task type.
	AwaitRequestAck(taskType enum.TaskType) (string, error)
}
