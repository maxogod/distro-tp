package integration_test

import (
	"testing"
	"time"

	"github.com/maxogod/distro-tp/src/common/middleware"
	"github.com/stretchr/testify/assert"
)

// AssertMiddleware1to1Works asserts that a middleware works with 1 sender and 1 receiver.
func AssertMiddleware1to1Works(sender, receiver middleware.MessageMiddleware, messagesToWait int, t *testing.T) {
	done := make(chan bool, 1)

	e := receiver.StartConsuming(func(consumeChannel middleware.ConsumeChannel, d chan error) {
		messagesReceived := 0
		for msg := range consumeChannel {
			t.Log("Received a message:", string(msg.Body))

			assert.Equal(t, "Hello World!", string(msg.Body))
			msg.Ack(false)

			d <- nil
			messagesReceived++
			if messagesReceived == messagesToWait {
				break
			}
		}
		done <- true
	})
	assert.Equal(t, 0, int(e))

	for range messagesToWait {
		e = sender.Send([]byte("Hello World!"))
		assert.Equal(t, 0, int(e))
	}

	// Check if consumer finished
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("did not receive message in time")
	}

	e = receiver.StopConsuming()
	assert.Equal(t, 0, int(e))

	e = receiver.Delete()
	assert.Equal(t, 0, int(e))

	e = receiver.Close()
	assert.Equal(t, 0, int(e))
}
