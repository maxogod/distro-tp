package middleware

import (
	"context"
	"time"

	"github.com/google/uuid"
	"github.com/maxogod/distro-tp/common/logger"
	amqp "github.com/rabbitmq/amqp091-go"
)

func NewQueueMiddleware(url, queueName string) (MessageMiddleware, error) {
	m := &MessageMiddlewareQueue{}

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

	_, err = ch.QueueDeclare(
		queueName, // name
		true,      // durable (persistent queue)
		false,     // delete when unused
		false,     // exclusive
		false,     // no-wait
		nil,       // arguments
	)
	if err != nil {
		logger.GetLogger().Errorln("Failed to declare a queue:", err)
		return m, err
	}

	m.queueName = queueName
	m.conn = conn
	m.channel = ch

	return m, nil
}

func (mq *MessageMiddlewareQueue) StartConsuming(onMessageCallback onMessageCallback) (e MessageMiddlewareError) {
	consumerTag := uuid.New().String()
	mq.consumerTag = consumerTag

	// TODO: prefetch count and size (Qos)
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
		logger.GetLogger().Errorln("Failed to register a consumer:", err)
		return MessageMiddlewareMessageError
	}
	mq.consumeChannel = consumeChannel

	done := make(chan error, 1)
	go onMessageCallback(mq.consumeChannel, done)

	return 0
}

func (mq *MessageMiddlewareQueue) StopConsuming() (e MessageMiddlewareError) {
	if mq.consumerTag == "" {
		logger.GetLogger().Warnln("StopConsuming called but no consumer is active")
		return 0
	}
	err := mq.channel.Cancel(mq.consumerTag, false)
	if err != nil {
		logger.GetLogger().Errorln("Failed to cancel the consumer:", err)
		return MessageMiddlewareMessageError
	}
	mq.consumerTag = ""
	return 0
}

func (mq *MessageMiddlewareQueue) Send(message []byte) (e MessageMiddlewareError) {
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
		logger.GetLogger().Errorln("Failed to publish a message:", err)
		return MessageMiddlewareMessageError
	}

	logger.GetLogger().Debugln("Sent message:", string(message))
	return 0
}

func (mq *MessageMiddlewareQueue) Close() (e MessageMiddlewareError) {
	errCh := mq.channel.Close()
	errConn := mq.conn.Close()
	if errCh != nil || errConn != nil {
		logger.GetLogger().Errorln("Failed to close middleware connection")
		return MessageMiddlewareCloseError
	}
	return 0
}

func (mq *MessageMiddlewareQueue) Delete() (e MessageMiddlewareError) {
	_, err := mq.channel.QueueDelete(
		mq.queueName, // name
		false,        // ifUnused
		false,        // ifEmpty
		false,        // noWait
	)

	if err != nil {
		logger.GetLogger().Errorln("Failed to delete queue:", err)
		return MessageMiddlewareDeleteError
	}

	return 0
}
