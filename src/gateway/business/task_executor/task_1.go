package task_executor

type taskExecutor struct {
	dataPath string
}

func NewTaskExecutor(dataPath string) TaskExecutor {
	return &taskExecutor{
		dataPath: dataPath,
	}
}

func (t *taskExecutor) Task1() error {
	return nil
}

func (t *taskExecutor) Task2() error {
	return nil
}

func (t *taskExecutor) Task3() error {
	return nil
}

func (t *taskExecutor) Task4() error {
	return nil
}
