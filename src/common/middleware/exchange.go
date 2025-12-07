package middleware

import (
	"context"
	"time"

	"github.com/google/uuid"
	"github.com/maxogod/distro-tp/src/common/logger"
	amqp "github.com/rabbitmq/amqp091-go"
)

type messageMiddlewareExchange struct {
	url          string
	exchangeName string
	conn         MiddlewareConnection
	channel      MiddlewareChannel
	routeKeys    []string

	consumeChannel ConsumeChannel
	consumerTag    string
}

func NewExchangeMiddleware(url, exchangeName, exchangeType string, routingKeys []string) (MessageMiddleware, error) {
	m := &messageMiddlewareExchange{}

	if len(routingKeys) == 0 {
		routingKeys = []string{""} // Default
	}

	conn, err := amqp.Dial(url)
	if err != nil {
		logger.Logger.Errorln("Failed to connect to RabbitMQ:", err)
		return m, err
	}

	ch, err := conn.Channel()
	if err != nil {
		logger.Logger.Errorln("Failed to open a channel:", err)
		return m, err
	}

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
		logger.Logger.Errorln("Failed to declare an exchange:", err)
		return m, err
	}

	m.exchangeName = exchangeName
	m.url = url
	m.routeKeys = routingKeys
	m.conn = conn
	m.channel = ch

	return m, nil
}

func (me *messageMiddlewareExchange) StartConsuming(onMessageCallback onMessageCallback) (e MessageMiddlewareError) {
	q, err := me.channel.QueueDeclare(
		"",    // name
		false, // durable
		true,  // delete when unused
		true,  // exclusive
		false, // no-wait
		nil,   // arguments
	)
	if err != nil {
		logger.Logger.Errorln("Failed to declare a queue:", err)
		return MessageMiddlewareMessageError
	}

	for _, key := range me.routeKeys {
		logger.Logger.Debugf("Binding queue %s to exchange %s with routing key %s", q.Name, me.exchangeName, key)
		err = me.channel.QueueBind(
			q.Name,          // queue name
			key,             // routing key
			me.exchangeName, // exchange
			false,           // no-wait
			nil,
		)
		if err != nil {
			logger.Logger.Errorln("Failed to bind consumer queue to exchange:", err)
			return MessageMiddlewareMessageError
		}
	}

	consumerTag := uuid.New().String()
	me.consumerTag = consumerTag

	consumeChannel, err := me.channel.Consume(
		q.Name,      // queue
		consumerTag, // consumer
		false,       // auto-ack
		false,       // exclusive
		false,       // no-local
		false,       // no-wait
		nil,         // args
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

func (me *messageMiddlewareExchange) StopConsuming() (error MessageMiddlewareError) {
	if me.conn.IsClosed() {
		return MessageMiddlewareDisconnectedError
	}

	if me.consumerTag == "" {
		logger.Logger.Warnln("StopConsuming called but no consumer is active")
		return MessageMiddlewareSuccess
	}

	err := me.channel.Cancel(me.consumerTag, false)
	if err != nil {
		logger.Logger.Errorln("Failed to cancel the consumer:", err)
		return MessageMiddlewareMessageError
	}
	me.consumerTag = ""

	return MessageMiddlewareSuccess
}

func (me *messageMiddlewareExchange) Send(message []byte) MessageMiddlewareError {
	if me.conn.IsClosed() {
		logger.Logger.Errorln("Connection is closed")
		if err := me.tryReconnect(); err != nil {
			return MessageMiddlewareDisconnectedError
		}
		logger.Logger.Debugln("Reconnected to RabbitMQ")
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

func (me *messageMiddlewareExchange) Close() (error MessageMiddlewareError) {
	if !me.channel.IsClosed() {
		if err := me.channel.Close(); err != nil {
			logger.Logger.Errorln("Failed to close channel:", err)
			return MessageMiddlewareCloseError
		}
	}

	if !me.conn.IsClosed() {
		if err := me.conn.Close(); err != nil {
			logger.Logger.Errorln("Failed to close connection:", err)
			return MessageMiddlewareCloseError
		}
	}

	return MessageMiddlewareSuccess
}

func (me *messageMiddlewareExchange) Delete() (error MessageMiddlewareError) {
	err := me.channel.ExchangeDelete(
		me.exchangeName, // name
		false,           // ifUnused
		false,           // noWait
	)
	if err != nil {
		logger.Logger.Errorln("Failed to delete exchange:", err)
		return MessageMiddlewareDeleteError
	}

	logger.Logger.Debugln("Deleted exchange:", me.exchangeName)

	return MessageMiddlewareSuccess
}

func (me *messageMiddlewareExchange) tryReconnect() error {
	conn, err := amqp.Dial(me.url)
	if err != nil {
		logger.Logger.Errorln("Failed to connect to RabbitMQ:", err)
		return err
	}

	ch, err := conn.Channel()
	if err != nil {
		logger.Logger.Errorln("Failed to open a channel:", err)
		return err
	}

	me.conn = conn
	me.channel = ch

	return nil
}
