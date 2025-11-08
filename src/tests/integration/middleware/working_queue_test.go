package integration_test

import (
	"testing"

	"github.com/maxogod/distro-tp/src/common/logger"
	"github.com/maxogod/distro-tp/src/common/middleware"
	"github.com/stretchr/testify/assert"
)

func TestMain(m *testing.M) {
	logger.InitLogger(logger.LoggerEnvDevelopment)
	m.Run()
}

// TestWorkingQueue1push1consume tests the success case of a single producer single consumer
// approach to working queues middleware.
func TestWorkingQueue1push1consume(t *testing.T) {
	m, err := middleware.NewQueueMiddleware(url, "test_queue1to1")
	assert.NoError(t, err)

	AssertMiddleware1to1Works(m, m, 1, t)
}

// TestWorkingQueue1pushNconsume tests the success case of single producer multiple consumers
// approach to working queues middleware.
func TestWorkingQueue1pushNconsume(t *testing.T) {
	AssertMiddleware1toNWorks(func() middleware.MessageMiddleware {
		m, err := middleware.NewQueueMiddleware(url, "test_queue1toN")
		assert.NoError(t, err)
		return m
	}, 5, t)
}

// TestWorkingQueueNpush1consume tests the success case of multiple producers single consumer
// approach to working queues middleware.
func TestWorkingQueueNpush1consume(t *testing.T) {
	AssertMiddlewareNto1Works(func() middleware.MessageMiddleware {
		m, err := middleware.NewQueueMiddleware(url, "test_queueNto1")
		assert.NoError(t, err)
		return m
	}, 5, t)
}
