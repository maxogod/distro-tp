package middleware

import (
	"time"
)

const MIDDLEWARE_CONNECTION_RETRIES = 10
const WAIT_INTERVAL = 1 * time.Second

/* --- Worker Middlewares --- */

// GetFilterQueue retrieves the middleware that the controller uses to put work on the filter queues
func GetFilterQueue(url string) MessageMiddleware {
	return retryMiddlewareCreation(MIDDLEWARE_CONNECTION_RETRIES, WAIT_INTERVAL, func() (MessageMiddleware, error) {
		return NewQueueMiddleware(url, "filter")
	})
}

// GetGroupByQueue retrieves the middleware that the controller uses to put work on the group by queues
func GetGroupByQueue(url string) MessageMiddleware {
	return retryMiddlewareCreation(MIDDLEWARE_CONNECTION_RETRIES, WAIT_INTERVAL, func() (MessageMiddleware, error) {
		return NewQueueMiddleware(url, "groupby")
	})
}

// GetReducerQueue retrieves the middleware that the controller uses to put work on the reducer queues
func GetReducerQueue(url string) MessageMiddleware {
	return retryMiddlewareCreation(MIDDLEWARE_CONNECTION_RETRIES, WAIT_INTERVAL, func() (MessageMiddleware, error) {
		return NewQueueMiddleware(url, "reducer")
	})
}

// GetJoinerQueue retrieves the middleware that the controller uses to put work on the joiner queues
func GetJoinerQueue(url string) MessageMiddleware {
	return retryMiddlewareCreation(MIDDLEWARE_CONNECTION_RETRIES, WAIT_INTERVAL, func() (MessageMiddleware, error) {
		return NewQueueMiddleware(url, "joiner")
	})
}

// GetRefDataExchange retrieves the middleware for the given exchange
// to send or receive as fanout for joiners.
func GetRefDataExchange(url, receiverName string) MessageMiddleware {
	if receiverName == "" {
		return retryMiddlewareCreation(MIDDLEWARE_CONNECTION_RETRIES, WAIT_INTERVAL, func() (MessageMiddleware, error) {
			return NewExchangeMiddleware(url, "ref_data_exchange", "fanout", []string{})
		})
	}
	return retryMiddlewareCreation(MIDDLEWARE_CONNECTION_RETRIES, WAIT_INTERVAL, func() (MessageMiddleware, error) {
		return NewPersistentExchangeMiddleware(url, "ref_data_exchange", "fanout", []string{}, "ref-"+receiverName)
	})
}

// GetAggregatorQueue retrieves the middleware that the controller uses to put work on the aggregator queues
func GetAggregatorQueue(url string, receiverName string) MessageMiddleware {
	if receiverName == "" {
		return retryMiddlewareCreation(MIDDLEWARE_CONNECTION_RETRIES, WAIT_INTERVAL, func() (MessageMiddleware, error) {
			return NewExchangeMiddleware(url, "aggregator", "fanout", []string{})
		})
	}

	return retryMiddlewareCreation(MIDDLEWARE_CONNECTION_RETRIES, WAIT_INTERVAL, func() (MessageMiddleware, error) {
		return NewPersistentExchangeMiddleware(url, "aggregator", "fanout", []string{}, "aggregator-"+receiverName)
	})
}

/* --- Processed Data Queue --- */

// GetProcessedDataQueue retrieves the middleware that the controller pops from to send the data back to the user.
// Filters in case of task 1 and aggregators in case of the other tasks will be the producers.
func GetProcessedDataExchange(url, clientID string) MessageMiddleware {
	return retryMiddlewareCreation(MIDDLEWARE_CONNECTION_RETRIES, WAIT_INTERVAL, func() (MessageMiddleware, error) {
		return NewExchangeMiddleware(url, "processed_data_exchange", "direct", []string{clientID})
	})
}

/* --- Node tracking for Controller --- */

// GetInitControlQueue retrieves the middleware for the given exchange
// to send or receive control messages for initialization.
func GetInitControlQueue(url string) MessageMiddleware {
	return retryMiddlewareCreation(MIDDLEWARE_CONNECTION_RETRIES, WAIT_INTERVAL, func() (MessageMiddleware, error) {
		return NewQueueMiddleware(url, "init_control_queue")
	})
}

// GetClientControlExchange retrieves the middleware for the given exchange
// to send or receive ack/nack for a client.
func GetClientControlExchange(url, clientID string) MessageMiddleware {
	return retryMiddlewareCreation(MIDDLEWARE_CONNECTION_RETRIES, WAIT_INTERVAL, func() (MessageMiddleware, error) {
		return NewExchangeMiddleware(url, "client_control_exchange", "direct", []string{clientID})
	})
}

// GetCounterExchange retrieves the middleware for the given exchange
// to send or receive with a specific topic pass the clientID parameter.
func GetCounterExchange(url, clientID string) MessageMiddleware {
	return retryMiddlewareCreation(MIDDLEWARE_CONNECTION_RETRIES, WAIT_INTERVAL, func() (MessageMiddleware, error) {
		return NewPersistentExchangeMiddleware(url, "counter_exchange", "direct", []string{clientID}, "counter-"+clientID)
	})
}

// GetFinishExchange retrieves the middleware for the given exchange
// to send or receive with apackage specific topic pass the topics parameter.
// Possible topics: joiner, aggregator
func GetFinishExchange(url string, topic []string, receiverName string) MessageMiddleware {
	if receiverName == "" {
		return retryMiddlewareCreation(MIDDLEWARE_CONNECTION_RETRIES, WAIT_INTERVAL, func() (MessageMiddleware, error) {
			return NewExchangeMiddleware(url, "finish_exchange", "direct", topic)
		})
	}
	return retryMiddlewareCreation(MIDDLEWARE_CONNECTION_RETRIES, WAIT_INTERVAL, func() (MessageMiddleware, error) {
		return NewPersistentExchangeMiddleware(url, "finish_exchange", "direct", topic, "fin-"+receiverName)
	})
}

/* --- Utils --- */

func retryMiddlewareCreation(retries int, waitInterval time.Duration, newMiddleware func() (MessageMiddleware, error)) MessageMiddleware {
	var m MessageMiddleware
	var err error
	for range retries {
		m, err = newMiddleware()
		if err != nil {
			time.Sleep(waitInterval)
			continue
		} else {
			break
		}
	}

	if err != nil {
		panic("Could not connect to remote middleware")
	}

	return m
}
