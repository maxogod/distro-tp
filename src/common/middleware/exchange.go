package middleware

func NewExchangeMiddleware(exchangeName string) MessageMiddleware {
	return &MessageMiddlewareExchange{
		exchangeName: exchangeName,
	}
}

func (me *MessageMiddlewareExchange) StartConsuming(onMessageCallback onMessageCallback) (error MessageMiddlewareError) {
	return MessageMiddlewareCloseError
}

func (me *MessageMiddlewareExchange) StopConsuming() (error MessageMiddlewareError) {
	return MessageMiddlewareCloseError
}

func (me *MessageMiddlewareExchange) Send(message []byte) (error MessageMiddlewareError) {
	return MessageMiddlewareCloseError
}

func (me *MessageMiddlewareExchange) Close() (error MessageMiddlewareError) {
	return MessageMiddlewareCloseError
}

func (me *MessageMiddlewareExchange) Delete() (error MessageMiddlewareError) {
	return MessageMiddlewareCloseError
}
