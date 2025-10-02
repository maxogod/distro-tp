package file_service

// FileReader reads the given csv batch by batch and produces to the channel
type FileService[T any] interface {
	ReadAsBatches(csvPath string, batches_ch chan []T, newObject func(record []string) T)
	SaveCsvAsBatches(csvPath string, batches_ch chan string, header string) error
}
