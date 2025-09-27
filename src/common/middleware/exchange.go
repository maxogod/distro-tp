package middleware

import (
	"context"
	"time"

	"github.com/google/uuid"
	"github.com/maxogod/distro-tp/src/common/logger"
	amqp "github.com/rabbitmq/amqp091-go"
)

func NewExchangeMiddleware(url, exchangeName, exchangeType string, routingKeys []string) (MessageMiddleware, error) {
	m := &MessageMiddlewareExchange{}

	if len(routingKeys) == 0 {
		routingKeys = []string{""} // Default
	}

	conn, err := amqp.Dial(url)
	if err != nil {
		logger.GetLogger().Errorln("Failed to connect to RabbitMQ:", err)
		return m, err
	}

	ch, err := conn.Channel()
	if err != nil {
		logger.GetLogger().Errorln("Failed to open a channel:", err)
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
		logger.GetLogger().Errorln("Failed to declare an exchange:", err)
		return m, err
	}

	m.exchangeName = exchangeName
	m.routeKeys = routingKeys
	m.conn = conn
	m.channel = ch

	return m, nil
}

func (me *MessageMiddlewareExchange) StartConsuming(onMessageCallback onMessageCallback) (e MessageMiddlewareError) {
	q, err := me.channel.QueueDeclare(
		"",    // name
		false, // durable
		false, // delete when unused
		true,  // exclusive
		false, // no-wait
		nil,   // arguments
	)
	if err != nil {
	}

	for _, key := range me.routeKeys {
		logger.GetLogger().Debugf("Binding queue %s to exchange %s with routing key %s", q.Name, me.exchangeName, key)
		err = me.channel.QueueBind(
			q.Name,          // queue name
			key,             // routing key
			me.exchangeName, // exchange
			false,           // no-wait
			nil,
		)
		if err != nil {
			logger.GetLogger().Errorln("Failed to bind consumer queue to exchange:", err)
			return MessageMiddlewareMessageError
		}
	}

	// TODO: prefetch count and size (Qos)
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
		logger.GetLogger().Errorln("Failed to register a consumer:", err)
		return MessageMiddlewareMessageError
	}

	me.consumeChannel = consumeChannel
	me.consumerQueueName = q.Name

	done := make(chan error, 1)
	go onMessageCallback(me.consumeChannel, done)

	return MessageMiddlewareSuccess
}

func (me *MessageMiddlewareExchange) StopConsuming() (error MessageMiddlewareError) {
	if me.consumerTag == "" {
		logger.GetLogger().Warnln("StopConsuming called but no consumer is active")
		return MessageMiddlewareSuccess
	}

	err := me.channel.Cancel(me.consumerTag, false)
	if err != nil {
		logger.GetLogger().Errorln("Failed to cancel the consumer:", err)
		return MessageMiddlewareMessageError
	}
	me.consumerTag = ""

	_, err = me.channel.QueueDelete(
		me.consumerQueueName, // name
		false,                // ifUnused
		false,                // ifEmpty
		false,                // noWait
	)
	if err != nil {
		logger.GetLogger().Errorln("Failed to delete queue:", err)
		return MessageMiddlewareDeleteError
	}

	return MessageMiddlewareSuccess
}

func (me *MessageMiddlewareExchange) Send(message []byte) MessageMiddlewareError {
	if me.conn.IsClosed() {
		logger.GetLogger().Errorln("Connection is closed")
		return MessageMiddlewareDisconnectedError
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	for _, key := range me.routeKeys {
		logger.GetLogger().Debugf("Publishing message to route %s: %s", key, string(message))
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
			logger.GetLogger().Errorf("Failed to publish a message to route %s: %v", key, err)
			return MessageMiddlewareMessageError
		}
	}

	logger.GetLogger().Debugln("Sent message:", string(message))

	return MessageMiddlewareSuccess
}

func (me *MessageMiddlewareExchange) Close() (error MessageMiddlewareError) {
	errCh := me.channel.Close()
	errConn := me.conn.Close()
	if errCh != nil || errConn != nil {
		logger.GetLogger().Errorln("Failed to close middleware connection")
		return MessageMiddlewareCloseError
	}

	return MessageMiddlewareSuccess
}

func (me *MessageMiddlewareExchange) Delete() (error MessageMiddlewareError) {
	err := me.channel.ExchangeDelete(
		me.exchangeName, // name
		false,           // ifUnused
		false,           // noWait
	)
	if err != nil {
		logger.GetLogger().Errorln("Failed to delete queue:", err)
		return MessageMiddlewareDeleteError
	}

	logger.GetLogger().Debugln("Deleted exchange:", me.exchangeName)

	return MessageMiddlewareSuccess
}
