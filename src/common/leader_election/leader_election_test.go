package leader_election_test

import (
	"testing"
	"time"

	"github.com/maxogod/distro-tp/src/common/logger"
)

var url = "amqp://guest:guest@localhost:5672/"
var sleepTime = time.Second

func TestMain(t *testing.M) {
	logger.InitLogger(logger.LoggerEnvDevelopment)
	t.Run()
}

func TestSingleNode(t *testing.T) {
	// le := leader_election.NewLeaderElection(1, url, enum.Gateway)
	// go le.Start(nil)
	// time.Sleep(sleepTime)
	// assert.True(t, le.IsLeader(), "Expected single node to be leader")
}
