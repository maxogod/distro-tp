package task_executor

const (
	TransactionsDirPath     = "/transactions"
	TransactionItemsDirPath = "/transaction_items"
	MenuItemsDirPath        = "/menu_items"
	StoresDirPath           = "/stores"
	UsersDirPath            = "/users"
)

type TaskExecutor interface {
	Task1() error
	Task2() error
	Task3() error
	Task4() error
}
