package enum

type RefDatasetType int

const (
	NoRef RefDatasetType = iota + 1
	MenuItems
	Stores
	Users
)
