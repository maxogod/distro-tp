package integration_test

import (
	"testing"
	"time"

	"github.com/maxogod/distro-tp/common/middleware"
	"github.com/stretchr/testify/assert"
)

func TestMain(t *testing.M) {
	t.Run()
}

// TestWorkingQueue1push1consume tests the success case of a single producer single consumer
// approach to working queues middleware.
func TestWorkingQueue1push1consume(t *testing.T) {
	url := "amqp://guest:guest@localhost:5672/"
	m, err := middleware.NewQueueMiddleware(url, "test_queue1to1")
	assert.NoError(t, err)

	done := make(chan error, 1)

	e := m.StartConsuming(func(consumeChannel middleware.ConsumeChannel, d chan error) {
		for msg := range consumeChannel {
			t.Log("Received a message:", string(msg.Body))
			assert.Equal(t, "Hello World!", string(msg.Body))
			msg.Ack(false)
			d <- nil
			break
		}
		done <- nil
	})
	assert.Equal(t, 0, int(e))

	e = m.Send([]byte("Hello World!"))
	assert.Equal(t, 0, int(e))

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

// TestWorkingQueue1pushNconsume tests the success case of single produce multiple consumers
// approach to working queues middleware.
func TestWorkingQueue1pushNconsume(t *testing.T) {
	url := "amqp://guest:guest@localhost:5672/"
	num_consumers := 5

	for range num_consumers {
		go func(t *testing.T) {
			m, err := middleware.NewQueueMiddleware(url, "test_queue1toN")
			assert.NoError(t, err)

			done := make(chan error, 1)

			e := m.StartConsuming(func(consumeChannel middleware.ConsumeChannel, d chan error) {
				for msg := range consumeChannel {
					t.Log("Received a message:", string(msg.Body))
					assert.Equal(t, "Hello World!", string(msg.Body))
					msg.Ack(false)
					d <- nil
					break
				}
				done <- nil
			})
			assert.Equal(t, 0, int(e))
			select {
			case <-done:
			case <-time.After(2 * time.Second):
				t.Fatal("did not receive message in time")
			}
		}(t)
	}

	m, err := middleware.NewQueueMiddleware(url, "test_queue1toN")
	assert.NoError(t, err)

	for range num_consumers {
		e := m.Send([]byte("Hello World!"))
		assert.Equal(t, 0, int(e))
	}

	time.Sleep(10 * time.Second)

	e := m.Delete()
	assert.Equal(t, 0, int(e))

	e = m.Close()
	assert.Equal(t, 0, int(e))
}
