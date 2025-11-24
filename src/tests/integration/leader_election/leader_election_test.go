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

	le1 := leader_election.NewLeaderElection("localhost", 9091, 1, url, enum.None, maxNodes, nil)
	assert.False(t, le1.IsLeader(), "Expected node to not be leader")

	time.Sleep(sleepTime)

	assert.True(t, le1.IsLeader(), "Expected node to be leader")

	le1.Close()
}

func TestTwoNodes(t *testing.T) {
	maxNodes := 2
	le1 := leader_election.NewLeaderElection("localhost", 9091, 1, url, enum.None, maxNodes, nil)
	le2 := leader_election.NewLeaderElection("localhost", 9092, 2, url, enum.None, maxNodes, nil)
	time.Sleep(5 * time.Second) // wait for connections to establish

	go le1.Start()
	go le2.Start()

	assert.False(t, le1.IsLeader(), "Expected node to not be leader")
	assert.False(t, le2.IsLeader(), "Expected node to not be leader")

	time.Sleep(sleepTime)

	assert.False(t, le1.IsLeader(), "Expected node to be leader")
	assert.True(t, le2.IsLeader(), "Expected node to be leader")

	le1.Close()
	le2.Close()
}

/* --- UTILS --- */

func TestTwoNodesWithMaxTenNodes(t *testing.T) {
	maxNodes := 10

	le1 := leader_election.NewLeaderElection("localhost", 9091, 1, url, enum.None, maxNodes, nil)
	le2 := leader_election.NewLeaderElection("localhost", 9092, 2, url, enum.None, maxNodes, nil)
	time.Sleep(5 * time.Second) // wait for connections to establish

	go le1.Start()
	go le2.Start()

	assert.False(t, le1.IsLeader(), "Expected node to not be leader")
	assert.False(t, le2.IsLeader(), "Expected node to not be leader")

	time.Sleep(sleepTime)

	assert.False(t, le1.IsLeader(), "Expected node to be leader")
	assert.True(t, le2.IsLeader(), "Expected node to be leader")

	le1.Close()
	le2.Close()
}

func TestNewNodeConnection(t *testing.T) {
	maxNodes := 10

	le1 := leader_election.NewLeaderElection("localhost", 9091, 1, url, enum.None, maxNodes, nil)
	le2 := leader_election.NewLeaderElection("localhost", 9092, 2, url, enum.None, maxNodes, nil)
	time.Sleep(5 * time.Second) // wait for connections to establish

	go le1.Start()
	go le2.Start()

	assert.False(t, le1.IsLeader(), "Expected node to not be leader")
	assert.False(t, le2.IsLeader(), "Expected node to not be leader")

	time.Sleep(sleepTime)

	assert.False(t, le1.IsLeader(), "Expected node to be leader")
	assert.True(t, le2.IsLeader(), "Expected node to be leader")

	// Connect a new node with higher ID
	le3 := leader_election.NewLeaderElection("localhost", 9093, 3, url, enum.None, maxNodes, nil)
	go le3.Start()
	time.Sleep(sleepTime)

	assert.False(t, le1.IsLeader(), "Expected node to be leader")
	assert.True(t, le2.IsLeader(), "Expected node to be leader")
	assert.False(t, le3.IsLeader(), "Expected new node to not be leader")

	le1.Close()
	le2.Close()
	le3.Close()
}

func TestNewNodeConnectionWithNewLeader(t *testing.T) {
	maxNodes := 10

	le1 := leader_election.NewLeaderElection("localhost", 9091, 1, url, enum.None, maxNodes, nil)
	le2 := leader_election.NewLeaderElection("localhost", 9092, 2, url, enum.None, maxNodes, nil)
	time.Sleep(5 * time.Second) // wait for connections to establish

	go le1.Start()
	go le2.Start()

	assert.False(t, le1.IsLeader(), "Expected node to not be leader")
	assert.False(t, le2.IsLeader(), "Expected node to not be leader")

	time.Sleep(sleepTime)

	assert.False(t, le1.IsLeader(), "Expected node to be leader")
	assert.True(t, le2.IsLeader(), "Expected node to be leader")

	// Connect a new node with higher ID
	le3 := leader_election.NewLeaderElection("localhost", 9093, 3, url, enum.None, maxNodes, nil)
	go le3.Start()
	time.Sleep(sleepTime)

	assert.False(t, le1.IsLeader(), "Expected node to be leader")
	assert.True(t, le2.IsLeader(), "Expected node to be leader")
	assert.False(t, le3.IsLeader(), "Expected new node to not be leader")

	// Close the leader node
	le2.Close()

	time.Sleep(sleepTime)

	assert.False(t, le1.IsLeader(), "Expected node to not be leader")
	assert.True(t, le3.IsLeader(), "Expected new node to be leader")
}

func TestNewNodeConnectionDuringElection(t *testing.T) {
	maxNodes := 10

	le1 := leader_election.NewLeaderElection("localhost", 9091, 1, url, enum.None, maxNodes, nil)
	le2 := leader_election.NewLeaderElection("localhost", 9092, 2, url, enum.None, maxNodes, nil)
	time.Sleep(5 * time.Second) // wait for connections to establish

	go le1.Start()
	go le2.Start()

	assert.False(t, le1.IsLeader(), "Expected node to not be leader")
	assert.False(t, le2.IsLeader(), "Expected node to not be leader")

	time.Sleep(sleepTime)

	assert.False(t, le1.IsLeader(), "Expected node to be leader")
	assert.True(t, le2.IsLeader(), "Expected node to be leader")

	// Connect a new node with higher ID
	le3 := leader_election.NewLeaderElection("localhost", 9093, 3, url, enum.None, maxNodes, nil)
	le2.Close()
	go le3.Start()
	time.Sleep(sleepTime)

	assert.True(t, le1.IsLeader(), "Expected node to be leader")
	assert.False(t, le3.IsLeader(), "Expected new node to not be leader")
}
