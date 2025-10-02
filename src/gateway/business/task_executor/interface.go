package task_executor

const (
	TransactionsDirPath     = "/transactions"
	TransactionItemsDirPath = "/transaction_items"
	MenuItemsDirPath        = "/menu_items"
	StoresDirPath           = "/stores"
	UsersDirPath            = "/users"
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
}
