package integration_test

import (
	"testing"
	"time"

	"github.com/maxogod/distro-tp/src/common/middleware"
	"github.com/stretchr/testify/assert"
)

// AssertMiddleware1to1Works asserts that a middleware works with 1 sender and 1 receiver.
func AssertMiddleware1to1Works(sender, receiver middleware.MessageMiddleware, messagesToWait int, t *testing.T) {
	defer receiver.StopConsuming()
	defer receiver.Close()
	defer sender.Delete()
	defer sender.Close()

	done := make(chan bool, 1)

	e := receiver.StartConsuming(func(consumeChannel middleware.ConsumeChannel, d chan error) {
		messagesReceived := 0
		for msg := range consumeChannel {
			t.Log("Received a message:", string(msg.Body))

			assert.Equal(t, "Hello World!", string(msg.Body))
			msg.Ack(false)

			messagesReceived++
			if messagesReceived == messagesToWait {
				break
			}
		}
		done <- true
	})
	assert.Equal(t, 0, int(e))

	e = sender.Send([]byte("Hello World!"))
	assert.Equal(t, 0, int(e))

	// Check if consumer finished
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("did not receive message in time")
	}
}

// AssertMiddleware1toNWorks asserts that a middleware works with 1 sender and N receivers.
func AssertMiddleware1toNWorks(newMiddleware func() middleware.MessageMiddleware, num_consumers int, t *testing.T) {
	consumers_ch := make(chan bool, num_consumers)
	ready_ch := make(chan bool, num_consumers)

	// Launch N independent consumers
	for consumer := range num_consumers {
		go func() {
			m := newMiddleware()
			defer m.StopConsuming()
			defer m.Close()

			done_ch := make(chan bool, num_consumers)

			e := m.StartConsuming(func(consumeChannel middleware.ConsumeChannel, d chan error) {
				ready_ch <- true
				for msg := range consumeChannel {
					t.Logf("[Consumer %d] Received a message: %s", consumer, string(msg.Body))
					assert.Equal(t, "Hello World!", string(msg.Body))
					msg.Ack(false)
					break
				}
				consumers_ch <- true
				done_ch <- true
			})
			assert.Equal(t, 0, int(e))

			<-done_ch // Wait until message then close
		}()
	}

	for range num_consumers {
		<-ready_ch
	}

	sender := newMiddleware()
	defer sender.Delete()
	defer sender.Close()

	// Send N messages for the N consumers
	for range num_consumers {
		e := sender.Send([]byte("Hello World!"))
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

}

func AssertMiddlewareNto1Works(newMiddleware func() middleware.MessageMiddleware, num_producers int, t *testing.T) {
	m := newMiddleware()
	defer m.StopConsuming()
	defer m.Delete()
	defer m.Close()

	done := make(chan bool, num_producers)
	ready := make(chan bool, 1)

	e := m.StartConsuming(func(consumeChannel middleware.ConsumeChannel, d chan error) {
		ready <- true
		for msg := range consumeChannel {
			t.Log("Received a message:", string(msg.Body))
			assert.Equal(t, "Hello World!", string(msg.Body))
			msg.Ack(false)
			done <- true
		}
	})
	assert.Equal(t, 0, int(e))

	<-ready // Wait until consumer is ready

	// Launch N independent producers
	for range num_producers {
		go func() {
			m := newMiddleware()
			defer m.Close()

			e := m.Send([]byte("Hello World!"))
			assert.Equal(t, 0, int(e))
		}()
	}

	// Get N messages from the N producers
	for range num_producers {
		select {
		case <-done:
		case <-time.After(2 * time.Second):
			t.Fatal("did not receive message in time")
		}
	}
}
