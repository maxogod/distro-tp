package integration_test

import (
	"testing"
	"time"

	"github.com/maxogod/github.com/maxogod/distro-tp/src/common/middleware"
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

// TestWorkingQueue1pushNconsume tests the success case of single producer multiple consumers
// approach to working queues middleware.
func TestWorkingQueue1pushNconsume(t *testing.T) {
	url := "amqp://guest:guest@localhost:5672/"
	num_consumers := 5
	consumers_ch := make(chan bool, num_consumers)

	// Launch N independent consumers
	for consumer := range num_consumers {
		go func() {
			m, err := middleware.NewQueueMiddleware(url, "test_queue1toN")
			assert.NoError(t, err)

			done_ch := make(chan bool, num_consumers)

			e := m.StartConsuming(func(consumeChannel middleware.ConsumeChannel, d chan error) {
				for msg := range consumeChannel {
					t.Logf("[Consumer %d] Received a message: %s", consumer, string(msg.Body))
					assert.Equal(t, "Hello World!", string(msg.Body))
					msg.Ack(false)
					d <- nil
					break
				}
				consumers_ch <- true
				done_ch <- true
			})
			assert.Equal(t, 0, int(e))

			<-done_ch // Wait until message then close
			e = m.StopConsuming()
			assert.Equal(t, 0, int(e))

			e = m.Close()
			assert.Equal(t, 0, int(e))
		}()
	}

	m, err := middleware.NewQueueMiddleware(url, "test_queue1toN")
	assert.NoError(t, err)

	// Send N messages for the N consumers
	for range num_consumers {
		e := m.Send([]byte("Hello World!"))
		assert.Equal(t, 0, int(e))
	}

	// Check if all consumers finished
	for range num_consumers {
		select {
		case <-consumers_ch:
		case <-time.After(2 * time.Second):
			t.Fatal("did not receive message in time")
		}
	}

	e := m.Delete()
	assert.Equal(t, 0, int(e))

	e = m.Close()
	assert.Equal(t, 0, int(e))
}

// TestWorkingQueueNpush1consume tests the success case of multiple producers single consumer
// approach to working queues middleware.
func TestWorkingQueueNpush1consume(t *testing.T) {
	url := "amqp://guest:guest@localhost:5672/"
	num_producers := 5

	// Launch N independent producers
	for range num_producers {
		go func() {
			m, err := middleware.NewQueueMiddleware(url, "test_queueNto1")
			assert.NoError(t, err)

			e := m.Send([]byte("Hello World!"))
			assert.Equal(t, 0, int(e))

			e = m.Close()
			assert.Equal(t, 0, int(e))
		}()
	}

	m, err := middleware.NewQueueMiddleware(url, "test_queueNto1")
	assert.NoError(t, err)

	done := make(chan bool, num_producers)

	e := m.StartConsuming(func(consumeChannel middleware.ConsumeChannel, d chan error) {
		for msg := range consumeChannel {
			t.Log("Received a message:", string(msg.Body))
			assert.Equal(t, "Hello World!", string(msg.Body))
			msg.Ack(false)
			done <- true
		}
		d <- nil
	})
	assert.Equal(t, 0, int(e))

	// Get N messages from the N producers
	for range num_producers {
		select {
		case <-done:
		case <-time.After(2 * time.Second):
			t.Fatal("did not receive message in time")
		}
	}

	e = m.Delete()
	assert.Equal(t, 0, int(e))

	e = m.Close()
	assert.Equal(t, 0, int(e))
}
