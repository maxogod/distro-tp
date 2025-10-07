package task_executor

import "fmt"

// TODO: review this config struct and see if in the future when adding more tasks,
// this should be replaced with a more generic approach
type TaskConfig struct {
	Persistence bool
}

func (tc TaskConfig) String() string {
	return fmt.Sprintf(
		"Persistence: %t",
		tc.Persistence,
	)
}
