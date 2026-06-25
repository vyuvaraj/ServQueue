package main

import (
	"context"
	"fmt"
	"net"
	"os"
	"testing"
	"time"

	"servqueue/pkg/broker"
)

func getFreePort(t *testing.T) int {
	addr, err := net.ResolveTCPAddr("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("getFreePort failed: %v", err)
	}
	l, err := net.ListenTCP("tcp", addr)
	if err != nil {
		t.Fatalf("getFreePort failed: %v", err)
	}
	defer l.Close()
	return l.Addr().(*net.TCPAddr).Port
}

func TestConsensusReplication(t *testing.T) {
	// Clean up any default WAL files
	_ = os.Remove("queue.wal")
	defer os.Remove("queue.wal")

	port1 := getFreePort(t)
	port2 := getFreePort(t)

	addr1 := fmt.Sprintf("127.0.0.1:%d", port1)
	addr2 := fmt.Sprintf("127.0.0.1:%d", port2)

	// Create leader engine & RaftNode
	leaderEngine := broker.NewBrokerEngine()
	defer leaderEngine.Stop()
	
	leaderRaft := broker.NewRaftNode(addr1, []string{addr2}, leaderEngine)
	if err := leaderRaft.Start(); err != nil {
		t.Fatalf("Failed to start leader RaftNode: %v", err)
	}
	defer leaderRaft.Close()
	leaderEngine.SetRaftNode(leaderRaft)
	leaderRaft.SetLeader(true)

	// Create follower engine & RaftNode
	followerEngine := broker.NewBrokerEngine()
	defer followerEngine.Stop()

	followerRaft := broker.NewRaftNode(addr2, []string{addr1}, followerEngine)
	if err := followerRaft.Start(); err != nil {
		t.Fatalf("Failed to start follower RaftNode: %v", err)
	}
	defer followerRaft.Close()
	followerEngine.SetRaftNode(followerRaft)
	followerRaft.SetLeader(false)

	// Sleep to allow listeners to start up
	time.Sleep(100 * time.Millisecond)

	// Test 1: Replicate Schema
	schema := map[string]string{
		"name": "string",
		"age":  "int",
	}
	leaderEngine.RegisterSchema(context.Background(), "user-events", schema)

	// Wait for replication
	time.Sleep(150 * time.Millisecond)

	followerSchema, exists := followerEngine.GetSchema("user-events")
	if !exists {
		t.Fatalf("Expected schema to be replicated to follower node, but it wasn't")
	}
	if followerSchema["name"] != "string" || followerSchema["age"] != "int" {
		t.Errorf("Replicated schema mismatch: got %v", followerSchema)
	}

	// Test 2: Replicate DLQ Configuration
	leaderEngine.SetDLQ(context.Background(), "user-events", "user-dlq")

	time.Sleep(150 * time.Millisecond)

	followerDLQ, ok := followerEngine.GetDLQ("user-events")
	if !ok || followerDLQ != "user-dlq" {
		t.Errorf("Expected DLQ to be replicated to follower, got %q (ok=%t)", followerDLQ, ok)
	}

	// Test 3: Replicate WASM Transform clearing (empty bytes)
	err := leaderEngine.RegisterTransform(context.Background(), "user-events", nil)
	if err != nil {
		t.Fatalf("Failed to register transform on leader: %v", err)
	}

	time.Sleep(150 * time.Millisecond)
	// Followers should not have transform
	topics := followerEngine.ListTopics()
	for _, top := range topics {
		if top.Name == "user-events" && top.HasTransform {
			t.Errorf("Expected transform to be cleared/absent on follower, but it has one")
		}
	}
}
