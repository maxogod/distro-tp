package integration_test

import (
	"testing"

	"github.com/maxogod/distro-tp/src/common/middleware"
	"github.com/stretchr/testify/assert"
)

// TestExchange1pub1sub tests the success case of a publisher-subscriber scenario with an exchange.
func TestExchange1pub1sub(t *testing.T) {
	url := "amqp://guest:guest@localhost:5672/"
	m, err := middleware.NewExchangeMiddleware(url, "test_exchange1to1", "fanout", []string{})
	assert.NoError(t, err)

	AssertMiddleware1to1Works(m, m, 1, t)
}

// TestExchange1pub1subMultipleKeys tests the success case of a publisher-subscriber scenario with an exchange and multiple routing keys.
func TestExchange1pub1subMultipleKeys(t *testing.T) {
	url := "amqp://guest:guest@localhost:5672/"
	m, err := middleware.NewExchangeMiddleware(url, "test_exchange1to1", "direct", []string{"key1", "key2", "key3"})
	assert.NoError(t, err)

	AssertMiddleware1to1Works(m, m, 3, t)
}
