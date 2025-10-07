package cache

import (
	"google.golang.org/protobuf/proto"
)

type messageHeap struct {
	data         []*proto.Message
	sortFunction func(a, b *proto.Message) bool
}

func (h messageHeap) Len() int           { return len(h.data) }
func (h messageHeap) Less(i, j int) bool { return h.sortFunction(h.data[i], h.data[j]) }
func (h messageHeap) Swap(i, j int)      { h.data[i], h.data[j] = h.data[j], h.data[i] }

func (h *messageHeap) Push(x interface{}) {
	h.data = append(h.data, x.(*proto.Message))
}

func (h *messageHeap) Pop() interface{} {
	old := h.data
	n := len(old)
	item := old[n-1]
	h.data = old[:n-1]
	return item
}
