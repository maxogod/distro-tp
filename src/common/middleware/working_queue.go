package middleware

import (
	"context"
	"time"

	"github.com/google/uuid"
	"github.com/maxogod/distro-tp/src/common/logger"

	amqp "github.com/rabbitmq/amqp091-go"
)

type messageMiddlewareQueue struct {
	conn    MiddlewareConnection
	channel MiddlewareChannel

	queueName      string
	consumeChannel ConsumeChannel
	consumerTag    string
}

func NewQueueMiddleware(url, queueName string) (MessageMiddleware, error) {
	m := &messageMiddlewareQueue{}

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

	q, err := ch.QueueDeclare(
		queueName, // name
		true,      // durable (persistent queue)
		false,     // delete when unused
		false,     // exclusive
		false,     // no-wait
		nil,       // arguments
	)
	if err != nil {
		logger.Logger.Errorln("Failed to declare a queue:", err)
		return m, err
	}

	m.queueName = q.Name
	m.conn = conn
	m.channel = ch

	return m, nil
}

func (mq *messageMiddlewareQueue) StartConsuming(onMessageCallback onMessageCallback) (e MessageMiddlewareError) {
	if mq.conn.IsClosed() {
		logger.Logger.Errorln("Connection is closed")
		return MessageMiddlewareDisconnectedError
	}

	consumerTag := uuid.New().String()
	mq.consumerTag = consumerTag

	consumeChannel, err := mq.channel.Consume(
		mq.queueName, // queue
		consumerTag,  // consumer
		false,        // auto-ack
		false,        // exclusive
		false,        // no-local
		false,        // no-wait
		nil,          // args
	)
	if err != nil {
		logger.Logger.Errorln("Failed to register a consumer:", err)
		return MessageMiddlewareMessageError
	}
	mq.consumeChannel = consumeChannel

	done := make(chan error, 1)
	go onMessageCallback(mq.consumeChannel, done)

	return MessageMiddlewareSuccess
}

func (mq *messageMiddlewareQueue) StopConsuming() (e MessageMiddlewareError) {
	if mq.conn.IsClosed() {
		return MessageMiddlewareDisconnectedError
	}

	if mq.consumerTag == "" {
		logger.Logger.Warnln("StopConsuming called but no consumer is active")
		return MessageMiddlewareSuccess
	}

	err := mq.channel.Cancel(mq.consumerTag, false)
	if err != nil {
		logger.Logger.Errorln("Failed to cancel the consumer:", err)
		return MessageMiddlewareMessageError
	}
	mq.consumerTag = ""

	return MessageMiddlewareSuccess
}

func (mq *messageMiddlewareQueue) Send(message []byte) (e MessageMiddlewareError) {
	if mq.conn.IsClosed() {
		logger.Logger.Errorln("Connection is closed")
		return MessageMiddlewareDisconnectedError
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	err := mq.channel.PublishWithContext(ctx,
		"",           // exchange
		mq.queueName, // routing key
		false,        // mandatory
		false,        // immediate
		amqp.Publishing{
			DeliveryMode: amqp.Persistent, // persistent messages
			ContentType:  "text/plain",
			Body:         message,
		})
	if err != nil {
		logger.Logger.Errorln("Failed to publish a message:", err)
		return MessageMiddlewareMessageError
	}

	return MessageMiddlewareSuccess
}

func (mq *messageMiddlewareQueue) Close() (e MessageMiddlewareError) {
	if !mq.channel.IsClosed() {
		if err := mq.channel.Close(); err != nil {
			logger.Logger.Errorln("Failed to close channel:", err)
			return MessageMiddlewareCloseError
		}
	}

	if !mq.conn.IsClosed() {
		if err := mq.conn.Close(); err != nil {
			logger.Logger.Errorln("Failed to close connection:", err)
			return MessageMiddlewareCloseError
		}
	}

	return MessageMiddlewareSuccess
}

func (mq *messageMiddlewareQueue) Delete() (e MessageMiddlewareError) {
	msg_count, err := mq.channel.QueueDelete(
		mq.queueName, // name
		false,        // ifUnused
		false,        // ifEmpty
		false,        // noWait
	)
	if err != nil {
		logger.Logger.Errorln("Failed to delete queue:", err)
		return MessageMiddlewareDeleteError
	}

	logger.Logger.Debugln("Deleted queue:", mq.queueName, "with", msg_count, "messages")

	return MessageMiddlewareSuccess
}
