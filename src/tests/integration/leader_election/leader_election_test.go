package leader_election_test

import (
	"testing"
	"time"

	"github.com/maxogod/distro-tp/src/common/leader_election"
	"github.com/maxogod/distro-tp/src/common/logger"
	"github.com/maxogod/distro-tp/src/common/models/enum"
	"github.com/stretchr/testify/assert"
)

var url = "amqp://guest:guest@localhost:5672/"
var sleepTime = time.Second * 10

func TestMain(t *testing.M) {
	logger.InitLogger(logger.LoggerEnvDevelopment)
	t.Run()
}

func TestSingleNode(t *testing.T) {
	maxNodes := 1

	le := leader_election.NewLeaderElection(1, url, enum.Gateway, maxNodes, nil)
	go le.Start()
	assert.False(t, le.IsLeader(), "Expected single node to not be leader")
	time.Sleep(sleepTime)
	assert.True(t, le.IsLeader(), "Expected single node to be leader")
}
