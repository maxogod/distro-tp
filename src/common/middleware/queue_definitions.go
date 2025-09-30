package middleware

import (
	"time"
)

const MIDDLEWARE_CONNECTION_RETRIES = 10

func GetFilterQueue(url string) MessageMiddleware {
	return retryMiddlewareCreation(MIDDLEWARE_CONNECTION_RETRIES, func() (MessageMiddleware, error) {
		return NewQueueMiddleware(url, "filter")
	})
}

/* --- Node tracking for Gateway Controller --- */

func GetNodeConnectionsQueue(url string) MessageMiddleware {
	return retryMiddlewareCreation(MIDDLEWARE_CONNECTION_RETRIES, func() (MessageMiddleware, error) {
		return NewQueueMiddleware(url, "node_connections")
	})
}

func GetFinishExchange(url string, subscriptionTopics []string) MessageMiddleware {
	return retryMiddlewareCreation(MIDDLEWARE_CONNECTION_RETRIES, func() (MessageMiddleware, error) {
		return NewExchangeMiddleware(url, "finish_exchange", "direct", subscriptionTopics)
	})
}

/* --- Reference Data Queues --- */

func GetMenuItemsQueue(url string) MessageMiddleware {
	return retryMiddlewareCreation(MIDDLEWARE_CONNECTION_RETRIES, func() (MessageMiddleware, error) {
		return NewQueueMiddleware(url, "menu_items")
	})
}

func GetStoresQueue(url string) MessageMiddleware {
	return retryMiddlewareCreation(MIDDLEWARE_CONNECTION_RETRIES, func() (MessageMiddleware, error) {
		return NewQueueMiddleware(url, "stores")
	})
}

func GetUsersQueue(url string) MessageMiddleware {
	return retryMiddlewareCreation(MIDDLEWARE_CONNECTION_RETRIES, func() (MessageMiddleware, error) {
		return NewQueueMiddleware(url, "users")
	})
}

/* --- Utils --- */

func retryMiddlewareCreation(retries int, newMiddleware func() (MessageMiddleware, error)) MessageMiddleware {
	var m MessageMiddleware
	var err error
	for range retries {
		m, err = newMiddleware()
		if err != nil {
			time.Sleep(1 * time.Second)
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
