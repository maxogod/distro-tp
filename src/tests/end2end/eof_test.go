package eof_test

import (
	"bytes"
	"fmt"
	"net/http"
	"os/exec"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/maxogod/distro-tp/src/common/models/enum"
	"github.com/maxogod/distro-tp/src/common/models/protocol"
	"github.com/maxogod/distro-tp/src/common/models/raw"
	mock "github.com/maxogod/distro-tp/src/tests/end2end/io"
	"google.golang.org/protobuf/proto"
)

var address = "localhost:8080"
var dockerComposeFile = "../../../docker-compose-test.yaml"
var healthCheckAddress = "localhost:8081/ping"

func TestSequentialRun(t *testing.T) {

	defer runCommand("docker", "compose", "-f", dockerComposeFile, "down")
	tests := []func(t *testing.T){
		testNoEof,
		testOnetoOneEof,
		testOnetoN,
		testNtoM,
	}

	t.Log("Setting up environment")
	runCommand("docker", "compose", "-f", dockerComposeFile, "up", "-d")
	runHealthCheck(t, healthCheckAddress)

	// Run each test one by one
	for i, test := range tests {
		t.Logf("========================[ Test %d ]========================", i+1)
		test(t)
		time.Sleep(100 * time.Millisecond)
	}
	t.Log("All tests completed!")
}

// -------- Helper functions for testing --------

func runCommand(name string, args ...string) (string, error) {

	cmd := exec.Command(name, args...)
	var out bytes.Buffer
	cmd.Stdout = &out
	cmd.Stderr = &out
	err := cmd.Run()
	return out.String(), err
}

func checkEOFLog(t *testing.T, container, clientId string) bool {
	finishLog := fmt.Sprintf("Finishing Client with ID: [%s]", clientId)
	out, err := runCommand("docker", "compose", "logs", container)
	if err != nil {
		t.Fatalf("Failed to get docker compose logs for %s: %v", container, err)
	}
	return strings.Contains(out, finishLog)
}

func runHealthCheck(t *testing.T, address string) error {
	const maxAttempts = 100
	const wait = 1 * time.Second
	for range maxAttempts {
		resp, err := http.Get(address)
		if err == nil && resp.StatusCode == http.StatusOK {
			t.Logf("Health check passed for %s", address)
			return nil
		}
		time.Sleep(wait)
	}
	return fmt.Errorf("health check failed for %s after %d attempts", address, maxAttempts)
}

func countLogMatches(t *testing.T, container string, pattern *regexp.Regexp) int {
	out, err := runCommand("docker", "compose", "logs", container)
	if err != nil {
		t.Fatalf("Failed to get docker compose logs for %s: %v", container, err)
	}
	return len(pattern.FindAllString(out, -1))
}

func getStoreDataBytes(t *testing.T) []byte {

	serializedStoreBatch, err := proto.Marshal(&mock.MockStoreRefData)
	if err != nil {
		t.Fatalf("Client failed to serialize store reference data: %v", err)
	}
	sockReferenceEnvelope := protocol.ReferenceEnvelope{
		Payload:       serializedStoreBatch,
		ReferenceType: int32(enum.Stores),
	}
	serializedRefEnvelope, err := proto.Marshal(&sockReferenceEnvelope)
	if err != nil {
		t.Fatalf("Client failed to serialize reference envelope: %v", err)
	}
	storeDataEnvelope := protocol.DataEnvelope{
		IsRef:    true,
		TaskType: int32(enum.T3),
		Payload:  serializedRefEnvelope,
	}
	serializedStoreEnvelope, err := proto.Marshal(&storeDataEnvelope)
	if err != nil {
		t.Fatalf("Client failed to serialize store data envelope: %v", err)
	}

	return serializedStoreEnvelope
}

func getEOFDataBytes(t *testing.T, taskType enum.TaskType, isRef bool) []byte {
	// Send EOF signal
	eofEnvelope := protocol.DataEnvelope{
		IsRef:    isRef,
		TaskType: int32(taskType),
		IsDone:   true,
	}
	serializedEofEnvelope, err := proto.Marshal(&eofEnvelope)
	if err != nil {
		t.Fatalf("Failed to serialize EOF envelope: %v", err)
	}
	return serializedEofEnvelope
}

func getDataBytes(t *testing.T, dataBatch *raw.TransactionBatch, taskType enum.TaskType) []byte {
	serializedBatch, err := proto.Marshal(dataBatch)
	if err != nil {
		t.Fatalf("Failed to serialize transaction: %v", err)
	}
	dataEnvelope := protocol.DataEnvelope{
		TaskType: int32(taskType),
		Payload:  serializedBatch,
	}
	serializedEnvelope, err := proto.Marshal(&dataEnvelope)
	if err != nil {
		t.Fatalf("Failed to serialize data envelope: %v", err)
	}
	return serializedEnvelope
}
