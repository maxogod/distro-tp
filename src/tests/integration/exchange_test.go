package integration_test

import (
	"testing"

	"github.com/maxogod/distro-tp/src/common/middleware"
	"github.com/stretchr/testify/assert"
)

var url = "amqp://guest:guest@localhost:5672/"

// TestExchange1pub1sub tests the success case of a publisher-subscriber scenario with an exchange.
func TestExchange1pub1sub(t *testing.T) {
	m, err := middleware.NewExchangeMiddleware(url, "test_exchange1to1", "fanout", []string{})
	assert.NoError(t, err)

	AssertMiddleware1to1Works(m, m, 1, t)
}

// TestExchange1pub1subMultipleKeys tests the success case of a publisher-subscriber scenario with an exchange and multiple routing keys.
func TestExchange1pub1subMultipleKeys(t *testing.T) {
	m, err := middleware.NewExchangeMiddleware(url, "test_exchange1to1_multiple", "direct", []string{"key1", "key2", "key3"})
	assert.NoError(t, err)

	AssertMiddleware1to1Works(m, m, 3, t)
}

// TestExchange1pubNsub tests the success case of a publisher-subscriber scenario with an exchange and multiple subscribers.
func TestExchange1pubNsub(t *testing.T) {
	// Uses fanout exchange to send messages to all subscribers at once
	AssertMiddleware1toNWorks(func() middleware.MessageMiddleware {
		m, err := middleware.NewExchangeMiddleware(url, "test_exchange1toN", "fanout", []string{})
		assert.NoError(t, err)
		return m
	}, 5, t)
}
