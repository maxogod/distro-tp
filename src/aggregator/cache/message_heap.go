package cache

import (
	"container/heap"

	"google.golang.org/protobuf/proto"
)

type messageHeap struct {
	data         []*proto.Message
	sortFunction func(a, b *proto.Message) bool
}

func NewMessageHeap(sortFn func(a, b *proto.Message) bool) heap.Interface {
	return &messageHeap{
		data:         []*proto.Message{},
		sortFunction: sortFn,
	}
}

func (h messageHeap) Len() int {
	return len(h.data)
}

func (h messageHeap) Less(i, j int) bool {
	return h.sortFunction(h.data[i], h.data[j])
}

func (h messageHeap) Swap(i, j int) {
	h.data[i], h.data[j] = h.data[j], h.data[i]
}

func (h *messageHeap) Push(x any) {
	h.data = append(h.data, x.(*proto.Message))
}

func (h *messageHeap) Pop() any {
	old := h.data
	n := len(old)
	item := old[n-1]
	h.data = old[:n-1]
	return item
}
