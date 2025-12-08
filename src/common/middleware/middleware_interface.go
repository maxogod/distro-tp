package middleware

import (
	amqp "github.com/rabbitmq/amqp091-go"
)

type MiddlewareConnection = *amqp.Connection
type MiddlewareChannel = *amqp.Channel
type MessageDelivery = amqp.Delivery
type ConsumeChannel = <-chan MessageDelivery

type MessageMiddlewareError int

const (
	MessageMiddlewareSuccess MessageMiddlewareError = iota
	MessageMiddlewareMessageError
	MessageMiddlewareDisconnectedError
	MessageMiddlewareCloseError
	MessageMiddlewareDeleteError
)

type onMessageCallback func(consumeChannel ConsumeChannel, done chan error)

type MessageMiddleware interface {
	/*
	   Starts listening to the queue/exchange and invokes the onMessageCallback
	   after each data or control message.
	   If the connection to the middleware is lost, it raises MessageMiddlewareDisconnectedError.
	   If an internal error occurs that cannot be resolved, it raises MessageMiddlewareMessageError.
	*/
	StartConsuming(onMessageCallback onMessageCallback) (e MessageMiddlewareError)

	/*
	   If it was consuming from the queue/exchange, it stops listening.
	   If it was not consuming from the queue/exchange, it has no effect and does not raise any error.
	   If the connection to the middleware is lost, it raises MessageMiddlewareDisconnectedError.
	*/
	StopConsuming() (e MessageMiddlewareError)

	/*
	   Sends a message to the queue or topic with which the exchange was initialized.
	   If the connection to the middleware is lost, it raises MessageMiddlewareDisconnectedError.
	   If an internal error occurs that cannot be resolved, it raises MessageMiddlewareMessageError.
	*/
	Send(message []byte) (e MessageMiddlewareError)

	/*
	   Disconnects from the queue or exchange to which it was connected.
	   If an internal error occurs that cannot be resolved, it raises MessageMiddlewareCloseError.
	*/
	Close() (e MessageMiddlewareError)

	/*
	   Forces the remote deletion of the queue or exchange.
	   If an internal error occurs that cannot be resolved, it raises MessageMiddlewareDeleteError.
	*/
	Delete() (e MessageMiddlewareError)
}
