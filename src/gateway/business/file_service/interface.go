package file_service

// FileReader reads the given csv batch by batch and produces to the channel
type FileService[T any] interface {
	ReadAsBatches(csv_path string, batches_ch chan []T, newObject func(record []string) T)
}
