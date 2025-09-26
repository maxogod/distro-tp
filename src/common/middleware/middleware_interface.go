package middleware

import (
	amqp "github.com/rabbitmq/amqp091-go"
)

type MiddlewareConnection = *amqp.Connection
type MiddlewareChannel = *amqp.Channel
type ConsumeChannel = <-chan amqp.Delivery

// TODO: why not use the actual error type?
type MessageMiddlewareError int

const (
	MessageMiddlewareSuccess MessageMiddlewareError = iota
	MessageMiddlewareMessageError
	MessageMiddlewareDisconnectedError
	MessageMiddlewareCloseError
	MessageMiddlewareDeleteError
)

type MessageMiddlewareQueue struct {
	queueName      string
	conn           MiddlewareConnection
	channel        MiddlewareChannel
	consumeChannel ConsumeChannel
	consumerTag    string
}

type MessageMiddlewareExchange struct {
	exchangeName      string
	routeKeys         []string
	conn              MiddlewareConnection
	channel           MiddlewareChannel
	consumerQueueName string
	consumeChannel    ConsumeChannel
	consumerTag       string
}

// TODO: check if done channel is really necessary (probably not)
type onMessageCallback func(consumeChannel ConsumeChannel, done chan error)

// Puede especificarse un tipo más específico para T si se desea
type MessageMiddleware interface {
	/*
	   Comienza a escuchar a la cola/exchange e invoca a onMessageCallback tras
	   cada mensaje de datos o de control.
	   Si se pierde la conexión con el middleware eleva MessageMiddlewareDisconnectedError.
	   Si ocurre un error interno que no puede resolverse eleva MessageMiddlewareMessageError.
	*/
	StartConsuming(onMessageCallback onMessageCallback) (e MessageMiddlewareError)

	/*
	   Si se estaba consumiendo desde la cola/exchange, se detiene la escucha. Si
	   no se estaba consumiendo de la cola/exchange, no tiene efecto, ni levanta
	   Si se pierde la conexión con el middleware eleva MessageMiddlewareDisconnectedError.
	*/
	StopConsuming() (e MessageMiddlewareError)

	/*
	   Envía un mensaje a la cola o al tópico con el que se inicializó el exchange.
	   Si se pierde la conexión con el middleware eleva MessageMiddlewareDisconnectedError.
	   Si ocurre un error interno que no puede resolverse eleva MessageMiddlewareMessageError.
	*/
	Send(message []byte) (e MessageMiddlewareError)

	/*
	   Se desconecta de la cola o exchange al que estaba conectado.
	   Si ocurre un error interno que no puede resolverse eleva MessageMiddlewareCloseError.
	*/
	Close() (e MessageMiddlewareError)

	/*
	   Se fuerza la eliminación remota de la cola o exchange.
	   Si ocurre un error interno que no puede resolverse eleva MessageMiddlewareDeleteError.
	*/
	Delete() (e MessageMiddlewareError)
}
