package middleware

type exchangeMiddleware struct{}

func NewExchangeMiddleware() MessageMiddleware[MessageMiddlewareExchange] {
	return &exchangeMiddleware{}
}

func (me *exchangeMiddleware) StartConsuming(m *MessageMiddlewareExchange, onMessageCallback onMessageCallback) (error MessageMiddlewareError) {
	return MessageMiddlewareCloseError
}

func (me *exchangeMiddleware) StopConsuming(m *MessageMiddlewareExchange) (error MessageMiddlewareError) {
	return MessageMiddlewareCloseError
}

func (me *exchangeMiddleware) Send(m *MessageMiddlewareExchange, message []byte) (error MessageMiddlewareError) {
	return MessageMiddlewareCloseError
}

func (me *exchangeMiddleware) Close(m *MessageMiddlewareExchange) (error MessageMiddlewareError) {
	return MessageMiddlewareCloseError
}

func (me *exchangeMiddleware) Delete(m *MessageMiddlewareExchange) (error MessageMiddlewareError) {
	return MessageMiddlewareCloseError
}
