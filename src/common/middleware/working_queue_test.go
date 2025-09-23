package middleware_test

import (
	"testing"
	"time"

	"github.com/maxogod/distro-tp/common/middleware"
	"github.com/stretchr/testify/assert"
)

func TestMain(t *testing.M) {
	t.Run()
}

func TestWorkingQueue1to1(t *testing.T) {
	url := "amqp://guest:guest@localhost:5672/"
	m, err := middleware.NewQueueMiddleware(url, "test_queue1to1")
	assert.NoError(t, err)

	done := make(chan error)
	e := m.StartConsuming(func(consumeChannel middleware.ConsumeChannel, d chan error) {
		done = d
		t.Log("Started consuming messages")
		for msg := range consumeChannel {
			t.Log("Received a message:", string(msg.Body))
			assert.Equal(t, string(msg.Body), "Hello, World!")
			msg.Ack(false)
			d <- nil
			return
		}
	})
	assert.Equal(t, 0, int(e))

	e = m.Send([]byte("Hello, World!"))
	assert.Equal(t, 0, int(e))

	select {
	case <-done:
		// success
	case <-time.After(2 * time.Second):
		t.Fatal("did not receive message in time")
	}

	e = m.StopConsuming()
	assert.Equal(t, 0, int(e))

	e = m.Close()
	assert.Equal(t, 0, int(e))

	e = m.Delete()
	assert.Equal(t, 0, int(e))
}
