package middleware

type queueMiddleware struct{}

func NewQueueMiddleware() MessageMiddleware[MessageMiddlewareQueue] {
	return &queueMiddleware{}
}

func (mq *queueMiddleware) StartConsuming(m *MessageMiddlewareQueue, onMessageCallback onMessageCallback) (error MessageMiddlewareError) {
	return MessageMiddlewareCloseError
}

func (mq *queueMiddleware) StopConsuming(m *MessageMiddlewareQueue) (error MessageMiddlewareError) {
	return MessageMiddlewareCloseError
}

func (mq *queueMiddleware) Send(m *MessageMiddlewareQueue, message []byte) (error MessageMiddlewareError) {
	return MessageMiddlewareCloseError
}

func (mq *queueMiddleware) Close(m *MessageMiddlewareQueue) (error MessageMiddlewareError) {
	return MessageMiddlewareCloseError
}

func (mq *queueMiddleware) Delete(m *MessageMiddlewareQueue) (error MessageMiddlewareError) {
	return MessageMiddlewareCloseError
}
