package middleware

import (
	"time"

	"github.com/maxogod/distro-tp/src/common/models/enum"
)

const MIDDLEWARE_CONNECTION_RETRIES = 10
const WAIT_INTERVAL = 1 * time.Second

/* --- Worker Queues --- */

// GetFilterQueue retrieves the middleware that the controller uses to put work on the filter queues
func GetFilterQueue(url string) MessageMiddleware {
	return retryMiddlewareCreation(MIDDLEWARE_CONNECTION_RETRIES, WAIT_INTERVAL, func() (MessageMiddleware, error) {
		return NewQueueMiddleware(url, "filter")
	})
}

// GetDataExchange retrieves the middleware used to send/receive data batches with the corresponding routing keys
// and to send a worker-type level broadcast for the DONE message.
func GetDataExchange(url string, subscriptionTopics []string) MessageMiddleware {
	return retryMiddlewareCreation(MIDDLEWARE_CONNECTION_RETRIES, WAIT_INTERVAL, func() (MessageMiddleware, error) {
		return NewExchangeMiddleware(url, "data_exchange", "direct", subscriptionTopics)
	})
}

/* --- Processed Data Queue --- */

// GetProcessedDataQueue retrieves the middleware that the controller pops from to send the data back to the user.
// Filters in case of task 1 and aggregators in case of the other tasks will be the producers.
func GetProcessedDataQueue(url string) MessageMiddleware {
	return retryMiddlewareCreation(MIDDLEWARE_CONNECTION_RETRIES, WAIT_INTERVAL, func() (MessageMiddleware, error) {
		return NewQueueMiddleware(url, "processed_data")
	})
}

/* --- Node tracking for Gateway Controller --- */

// GetNodeConnectionsQueue retrieves the middleware to be used by workers to tell
// controller they connected or they finished
func GetNodeConnectionsQueue(url string) MessageMiddleware {
	return retryMiddlewareCreation(MIDDLEWARE_CONNECTION_RETRIES, WAIT_INTERVAL, func() (MessageMiddleware, error) {
		return NewQueueMiddleware(url, "node_connections")
	})
}

// GetFinishExchange retrieves the middleware for the given exchange
// to send or receive with a specific topic pass the topics parameter.
// Possible topics: filter, groupby, reducer, joiner, aggregator
func GetFinishExchange(url string, workerType enum.WorkerType) MessageMiddleware {
	return retryMiddlewareCreation(MIDDLEWARE_CONNECTION_RETRIES, WAIT_INTERVAL, func() (MessageMiddleware, error) {
		return NewExchangeMiddleware(url, "data_exchange", "direct", []string{string(workerType)})
	})
}

/* --- Reference Data Queues --- */

// GetMenuItemsQueue retrieves the middleware used by controller to send menu_items reference data.
func GetMenuItemsQueue(url string) MessageMiddleware {
	return retryMiddlewareCreation(MIDDLEWARE_CONNECTION_RETRIES, WAIT_INTERVAL, func() (MessageMiddleware, error) {
		return NewQueueMiddleware(url, "menu_items")
	})
}

// GetStoresQueue retrieves the middleware used by controller to send stores reference data.
func GetStoresQueue(url string) MessageMiddleware {
	return retryMiddlewareCreation(MIDDLEWARE_CONNECTION_RETRIES, WAIT_INTERVAL, func() (MessageMiddleware, error) {
		return NewQueueMiddleware(url, "stores")
	})
}

// GetUsersQueue retrieves the middleware used by controller to send users reference data.
func GetUsersQueue(url string) MessageMiddleware {
	return retryMiddlewareCreation(MIDDLEWARE_CONNECTION_RETRIES, WAIT_INTERVAL, func() (MessageMiddleware, error) {
		return NewQueueMiddleware(url, "users")
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
