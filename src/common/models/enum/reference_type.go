package enum

type ReferenceType int32

const (
	MenuItems ReferenceType = iota + 1
	Stores
	Users
)
