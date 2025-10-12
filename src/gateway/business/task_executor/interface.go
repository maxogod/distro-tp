package task_executor

const (
	TransactionsDirPath     = "/transactions"
	TransactionItemsDirPath = "/transaction_items"
	MenuItemsDirPath        = "/menu_items"
	StoresDirPath           = "/stores"
	UsersDirPath            = "/users"

	OUTPUT_FILE_T1   = "task1_output.csv"
	OUTPUT_FILE_T2_1 = "task2_1_output.csv"
	OUTPUT_FILE_T2_2 = "task2_2_output.csv"
	OUTPUT_FILE_T3   = "task3_output.csv"
	OUTPUT_FILE_T4   = "task4_output.csv"
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
