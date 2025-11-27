package leader_election_test

import (
	"sync/atomic"
	"testing"
	"time"

	"github.com/maxogod/distro-tp/src/common/leader_election"
	"github.com/maxogod/distro-tp/src/common/logger"
	"github.com/maxogod/distro-tp/src/common/models/enum"
	"github.com/maxogod/distro-tp/src/common/models/protocol"
	"github.com/stretchr/testify/assert"
)

var url = "amqp://guest:guest@localhost:5672/"
var sleepTime = time.Second * 10

func TestMain(t *testing.M) {
	logger.InitLogger(logger.LoggerEnvDevelopment)
	t.Run()
}

type testUpdatesCallbacks struct {
	sent     *atomic.Bool
	updated  *atomic.Bool
	resetted *atomic.Bool
	t        *testing.T
}

func (u *testUpdatesCallbacks) SendUpdates(msgs chan *protocol.DataEnvelope, _ chan bool) {
	for i := range 3 {
		data := &protocol.DataEnvelope{
			SequenceNumber: int32(i),
		}
		msgs <- data
		time.Sleep(1 * time.Second)
	}
	data := &protocol.DataEnvelope{
		IsDone: true,
	}
	msgs <- data
	close(msgs)
	u.sent.Store(true)
}

func (u *testUpdatesCallbacks) GetUpdates(msgs chan *protocol.DataEnvelope) {
	i := 0
	for msg := range msgs {
		assert.Equal(u.t, msg.GetSequenceNumber(), int32(i), "Expected correct seq num")
		i++
	}
	u.updated.Store(true)
}

func (u *testUpdatesCallbacks) ResetUpdates() {
	u.resetted.Store(true)
}

func TestSingleNode(t *testing.T) {
	maxNodes := 1

	le1 := leader_election.NewLeaderElection("localhost", 9091, 1, url, enum.None, maxNodes, nil)
	go le1.Start()
	assert.False(t, le1.IsLeader(), "Expected node to not be leader")

	time.Sleep(sleepTime)

	assert.True(t, le1.IsLeader(), "Expected node to be leader")

	le1.Close()
}

func TestTwoNodes(t *testing.T) {
	maxNodes := 2
	le1 := leader_election.NewLeaderElection("localhost", 9091, 1, url, enum.None, maxNodes, nil)
	le2 := leader_election.NewLeaderElection("localhost", 9092, 2, url, enum.None, maxNodes, nil)

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

func TestTwoNodesWithMaxTenNodes(t *testing.T) {
	maxNodes := 10

	le1 := leader_election.NewLeaderElection("localhost", 9091, 1, url, enum.None, maxNodes, nil)
	le2 := leader_election.NewLeaderElection("localhost", 9092, 2, url, enum.None, maxNodes, nil)

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
	assert.False(t, le2.IsLeader(), "Expected node to be leader")
	assert.True(t, le3.IsLeader(), "Expected new node to not be leader")

	le1.Close()
	le2.Close()
	le3.Close()
}

func TestNewNodeConnectionWithNewLeader(t *testing.T) {
	maxNodes := 10

	le1 := leader_election.NewLeaderElection("localhost", 9091, 1, url, enum.None, maxNodes, nil)
	le2 := leader_election.NewLeaderElection("localhost", 9092, 2, url, enum.None, maxNodes, nil)

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
	assert.False(t, le2.IsLeader(), "Expected node to be leader")
	assert.True(t, le3.IsLeader(), "Expected new node to not be leader")

	// Close the leader node
	le3.Close()

	time.Sleep(sleepTime)

	assert.False(t, le1.IsLeader(), "Expected node to not be leader")
	assert.True(t, le2.IsLeader(), "Expected new node to be leader")

	le1.Close()
	le2.Close()
}

func TestNewNodeConnectionDuringElection(t *testing.T) {
	maxNodes := 10

	le1 := leader_election.NewLeaderElection("localhost", 9091, 1, url, enum.None, maxNodes, nil)
	le2 := leader_election.NewLeaderElection("localhost", 9092, 2, url, enum.None, maxNodes, nil)

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

	assert.False(t, le1.IsLeader(), "Expected node to not be leader")
	assert.True(t, le3.IsLeader(), "Expected new node to be leader")

	le1.Close()
	le3.Close()
}

func TestNewNodeUpdates(t *testing.T) {
	maxNodes := 3
	sent := &atomic.Bool{}
	resetted := &atomic.Bool{}
	updated := &atomic.Bool{}
	updatesCallbacks := &testUpdatesCallbacks{
		sent:     sent,
		updated:  updated,
		resetted: resetted,
		t:        t,
	}

	le1 := leader_election.NewLeaderElection("localhost", 9091, 1, url, enum.None, maxNodes, updatesCallbacks)
	le2 := leader_election.NewLeaderElection("localhost", 9092, 2, url, enum.None, maxNodes, updatesCallbacks)

	go le1.Start()
	go le2.Start()

	assert.False(t, le1.IsLeader(), "Expected node to not be leader")
	assert.False(t, le2.IsLeader(), "Expected node to not be leader")

	time.Sleep(sleepTime)

	assert.False(t, le1.IsLeader(), "Expected node to be leader")
	assert.True(t, le2.IsLeader(), "Expected node to be leader")

	// Connect a new node with higher ID
	le3 := leader_election.NewLeaderElection("localhost", 9093, 3, url, enum.None, maxNodes, updatesCallbacks)
	go le3.Start()
	time.Sleep(sleepTime)

	assert.False(t, le1.IsLeader(), "Expected node to be leader")
	assert.False(t, le2.IsLeader(), "Expected node to be leader")
	assert.True(t, le3.IsLeader(), "Expected new node to not be leader")

	assert.True(t, sent.Load(), "Expected updates to be sent")
	assert.True(t, updated.Load(), "Expected updates to be received")
	assert.True(t, resetted.Load(), "Expected updates to be resetted")

	le1.Close()
	le2.Close()
	le3.Close()
}

func TestNewNodeLeaderDiesMidUpdates(t *testing.T) {
	maxNodes := 3
	sent := &atomic.Bool{}
	resetted := &atomic.Bool{}
	updated := &atomic.Bool{}
	updatesCallbacks := &testUpdatesCallbacks{
		sent:     sent,
		updated:  updated,
		resetted: resetted,
		t:        t,
	}

	le1 := leader_election.NewLeaderElection("localhost", 9091, 1, url, enum.None, maxNodes, updatesCallbacks)
	le2 := leader_election.NewLeaderElection("localhost", 9092, 2, url, enum.None, maxNodes, updatesCallbacks)

	go le1.Start()
	go le2.Start()

	assert.False(t, le1.IsLeader(), "Expected node to not be leader")
	assert.False(t, le2.IsLeader(), "Expected node to not be leader")

	time.Sleep(sleepTime)

	assert.False(t, le1.IsLeader(), "Expected node to be leader")
	assert.True(t, le2.IsLeader(), "Expected node to be leader")

	// Connect a new node with higher ID
	le3 := leader_election.NewLeaderElection("localhost", 9093, 3, url, enum.None, maxNodes, updatesCallbacks)
	go le3.Start()

	time.Sleep(1 * time.Second)

	le2.Close() // Leader dies mid updates

	time.Sleep(3 * sleepTime)

	assert.False(t, le1.IsLeader(), "Expected node to be leader")
	assert.True(t, le3.IsLeader(), "Expected new node to not be leader")

	assert.True(t, sent.Load(), "Expected updates to be sent")
	assert.True(t, updated.Load(), "Expected updates to be received")
	assert.True(t, resetted.Load(), "Expected updates to be resetted")

	le1.Close()
	le3.Close()
}
