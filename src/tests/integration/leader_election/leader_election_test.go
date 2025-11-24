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

	le := createNewNode(1, 9091, maxNodes)
	assert.False(t, le.IsLeader(), "Expected single node to not be leader")

	time.Sleep(sleepTime)

	assert.True(t, le.IsLeader(), "Expected single node to be leader")

	le.Close()
}

func TestTwoNodes(t *testing.T) {
	maxNodes := 2

	le1 := createNewNode(1, 9091, maxNodes)
	le2 := createNewNode(2, 9092, maxNodes)
	assert.False(t, le1.IsLeader(), "Expected single node to not be leader")
	assert.False(t, le2.IsLeader(), "Expected single node to not be leader")

	time.Sleep(sleepTime)

	assert.False(t, le1.IsLeader(), "Expected single node to be leader")
	assert.True(t, le2.IsLeader(), "Expected single node to be leader")

	le1.Close()
	le2.Close()
}

/* --- UTILS --- */

func createNewNode(id int32, port int, maxNodes int) leader_election.LeaderElection {
	le := leader_election.NewLeaderElection("localhost", port, id, url, enum.None, maxNodes, nil)
	go le.Start()
	return le
}
