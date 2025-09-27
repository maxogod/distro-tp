package integration_test

import (
	"testing"
	"time"

	"github.com/maxogod/distro-tp/src/common/middleware"
	"github.com/stretchr/testify/assert"
)

// TestExchange1pub1sub tests the success case of a publisher-subscriber scenario with an exchange.
func TestExchange1pub1sub(t *testing.T) {
	url := "amqp://guest:guest@localhost:5672/"
	m, err := middleware.NewExchangeMiddleware(url, "test_exchange1to1", "fanout")
	assert.NoError(t, err)

	done := make(chan bool, 1)

	e := m.StartConsuming(func(consumeChannel middleware.ConsumeChannel, d chan error) {
		for msg := range consumeChannel {
			t.Log("Received a message:", string(msg.Body))
			assert.Equal(t, "Hello World!", string(msg.Body))
			msg.Ack(false)
			d <- nil
			break
		}
		done <- true
	})
	assert.Equal(t, 0, int(e))

	e = m.Send([]byte("Hello World!"))
	assert.Equal(t, 0, int(e))

	// Check if consumer finished
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("did not receive message in time")
	}

	e = m.StopConsuming()
	assert.Equal(t, 0, int(e))

	e = m.Delete()
	assert.Equal(t, 0, int(e))

	e = m.Close()
	assert.Equal(t, 0, int(e))
}
