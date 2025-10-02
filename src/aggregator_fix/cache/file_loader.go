package cache

import (
	"encoding/binary"
	"io"
	"os"

	"github.com/maxogod/distro-tp/src/common/models/data_batch"
	"google.golang.org/protobuf/proto"
)

type MergeFunc[T proto.Message] func(accumulated, incoming T) T
type KeyFunc[T proto.Message] func(item T) string

func AggregateData[T proto.Message, B proto.Message](
	f *os.File,
	createSpecificBatch func() B,
	getItems func(B) []T,
	merge MergeFunc[T],
	key KeyFunc[T],
) (map[string]T, error) {
	var length uint32
	if err := binary.Read(f, binary.LittleEndian, &length); err != nil {
		return nil, err
	}

	dataBatchBytes := make([]byte, length)
	if _, err := io.ReadFull(f, dataBatchBytes); err != nil {
		return nil, err
	}

	dataBatch := &data_batch.DataBatch{}
	err := proto.Unmarshal(dataBatchBytes, dataBatch)
	if err != nil {
		return nil, err
	}

	specificBatch := createSpecificBatch()
	if err = proto.Unmarshal(dataBatch.Payload, specificBatch); err != nil {
		return nil, err
	}

	aggregatedItems := make(map[string]T)
	for _, item := range getItems(specificBatch) {
		k := key(item)
		if existing, ok := aggregatedItems[k]; ok {
			aggregatedItems[k] = merge(existing, item)
		} else {
			aggregatedItems[k] = proto.Clone(item).(T)
		}
	}

	return aggregatedItems, nil
}
