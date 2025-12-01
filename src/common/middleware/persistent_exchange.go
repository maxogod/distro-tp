package middleware

import (
	"context"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/maxogod/distro-tp/src/common/logger"
	amqp "github.com/rabbitmq/amqp091-go"
)

func NewPersistentExchangeMiddleware(url, exchangeName, exchangeType string, routingKeys []string, queueName string) (MessageMiddleware, error) {
	m := &MessageMiddlewarePersistentExchange{
		exchangeName: exchangeName,
		queueName:    queueName,
		routeKeys:    routingKeys,
	}

	if len(routingKeys) == 0 {
		routingKeys = []string{""} // Default
	}

	conn, err := amqp.Dial(url)
	if err != nil {
		logger.Logger.Errorln("Failed to connect to RabbitMQ:", err)
		return m, err
	}
	m.conn = conn

	ch, err := conn.Channel()
	if err != nil {
		logger.Logger.Errorln("Failed to open a channel:", err)
		return m, err
	}
	m.channel = ch

	err = ch.ExchangeDeclare(
		exchangeName, // name
		exchangeType, // type
		true,         // durable (persistant exchange)
		false,        // auto-deleted
		false,        // internal
		false,        // no-wait
		nil,          // arguments
	)
	if err != nil {
		return m, fmt.Errorf("failed to declare an exchange: %w", err)
	}

	if queueName == "" {
		logger.Logger.Debugln("No queue name specified, skipping queue declaration and binding")
		return m, nil
	}

	q, err := ch.QueueDeclare(
		queueName, // name
		true,      // durable
		false,     // delete when unused
		false,     // exclusive
		false,     // no-wait
		nil,       // arguments
	)
	if err != nil {
		return m, fmt.Errorf("failed to declare a queue: %w", err)
	}

	for _, key := range routingKeys {
		logger.Logger.Debugf("Binding queue %s to exchange %s with routing key %s", q.Name, exchangeName, key)
		err = ch.QueueBind(
			q.Name,       // queue name
			key,          // routing key
			exchangeName, // exchange
			false,        // no-wait
			nil,
		)
		if err != nil {
			return m, fmt.Errorf("failed to bind queue to exchange: %w", err)
		}
	}

	return m, nil
}

func (me *MessageMiddlewarePersistentExchange) StartConsuming(onMessageCallback onMessageCallback) (e MessageMiddlewareError) {
	if me.queueName == "" {
		logger.Logger.Errorln("Cannot start consuming: no queue name specified on constructor")
		return MessageMiddlewareMessageError
	}

	consumerTag := uuid.New().String()
	me.consumerTag = consumerTag

	consumeChannel, err := me.channel.Consume(
		me.queueName, // queue
		consumerTag,  // consumer
		false,        // auto-ack
		true,         // exclusive
		false,        // no-local
		false,        // no-wait
		nil,          // args
	)
	if err != nil {
		logger.Logger.Errorln("Failed to register a consumer:", err)
		return MessageMiddlewareMessageError
	}

	me.consumeChannel = consumeChannel

	done := make(chan error, 1)
	go onMessageCallback(me.consumeChannel, done)

	return MessageMiddlewareSuccess
}

func (me *MessageMiddlewarePersistentExchange) StopConsuming() (e MessageMiddlewareError) {
	if me.consumerTag == "" {
		logger.Logger.Warnln("StopConsuming called but no consumer is active")
		return MessageMiddlewareSuccess
	}

	err := me.channel.Cancel(me.consumerTag, false)
	if err != nil {
		logger.Logger.Errorln("Failed to cancel the consumer:", err)
		return MessageMiddlewareCloseError
	}
	me.consumerTag = ""

	return MessageMiddlewareSuccess
}

func (me *MessageMiddlewarePersistentExchange) Send(message []byte) (e MessageMiddlewareError) {
	if me.conn.IsClosed() {
		logger.Logger.Errorln("Connection is closed")
		return MessageMiddlewareDisconnectedError
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	for _, key := range me.routeKeys {
		err := me.channel.PublishWithContext(ctx,
			me.exchangeName, // exchange
			key,             // routing key
			false,           // mandatory
			false,           // immediate
			amqp.Publishing{
				DeliveryMode: amqp.Persistent,
				ContentType:  "text/plain",
				Body:         message,
			})
		if err != nil {
			logger.Logger.Errorf("Failed to publish a message to route %s: %v", key, err)
			return MessageMiddlewareMessageError
		}
	}

	return MessageMiddlewareSuccess
}

func (me *MessageMiddlewarePersistentExchange) Close() (e MessageMiddlewareError) {
	if err := me.channel.Close(); err != nil {
		logger.Logger.Errorln("Failed to close channel:", err)
		return MessageMiddlewareCloseError
	}

	if err := me.conn.Close(); err != nil {
		logger.Logger.Errorln("Failed to close connection:", err)
		return MessageMiddlewareCloseError
	}

	return MessageMiddlewareSuccess
}

func (me *MessageMiddlewarePersistentExchange) Delete() (e MessageMiddlewareError) {
	err := me.channel.ExchangeDelete(
		me.exchangeName, // name
		false,           // if-unused
		false,           // no-wait
	)
	if err != nil {
		logger.Logger.Errorln("Failed to delete exchange:", err)
		return MessageMiddlewareDeleteError
	}

	msg_count, err := me.channel.QueueDelete(
		me.queueName, // name
		false,        // ifUnused
		false,        // ifEmpty
		false,        // noWait
	)
	if err != nil {
		logger.Logger.Errorln("Failed to delete queue:", err)
		return MessageMiddlewareDeleteError
	}

	logger.Logger.Debugln("Deleted queue:", me.queueName, "with", msg_count, "messages")

	return MessageMiddlewareSuccess
}
