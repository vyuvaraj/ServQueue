package broker

import (
	"context"
	"encoding/json"
	"log"
	"net"
	"sync"
)

type ClusterCommand struct {
	Op        string            `json:"op"`
	Topic     string            `json:"topic"`
	WasmBytes []byte            `json:"wasm_bytes,omitempty"`
	DLQTopic  string            `json:"dlq_topic,omitempty"`
	Schema    map[string]string `json:"schema,omitempty"`
}

type RaftNode struct {
	mu        sync.Mutex
	peers     []string
	isLeader  bool
	engine    *BrokerEngine
	listener  net.Listener
	addr      string
}

func NewRaftNode(addr string, peers []string, engine *BrokerEngine) *RaftNode {
	return &RaftNode{
		addr:   addr,
		peers:  peers,
		engine: engine,
	}
}

func (n *RaftNode) Start() error {
	l, err := net.Listen("tcp", n.addr)
	if err != nil {
		return err
	}
	n.listener = l

	// For simplicity, default first node as leader if no peers are specified
	if len(n.peers) == 0 {
		n.isLeader = true
	}

	go n.acceptConnections()
	return nil
}

func (n *RaftNode) SetLeader(isLeader bool) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.isLeader = isLeader
}

func (n *RaftNode) acceptConnections() {
	for {
		conn, err := n.listener.Accept()
		if err != nil {
			return
		}
		go n.handleConnection(conn)
	}
}

func (n *RaftNode) handleConnection(conn net.Conn) {
	defer conn.Close()
	var cmd ClusterCommand
	if err := json.NewDecoder(conn).Decode(&cmd); err != nil {
		return
	}

	// Replicate command to state machine
	ctx := context.WithValue(context.Background(), "replicated", true)
	switch cmd.Op {
	case "REGISTER_TRANSFORM":
		_ = n.engine.RegisterTransform(ctx, cmd.Topic, cmd.WasmBytes)
	case "SET_DLQ":
		n.engine.SetDLQ(ctx, cmd.Topic, cmd.DLQTopic)
	case "REGISTER_SCHEMA":
		n.engine.RegisterSchema(ctx, cmd.Topic, cmd.Schema)
	}
}

func (n *RaftNode) Replicate(op, topic string, wasmBytes []byte, dlqTopic string, schema map[string]string) {
	n.mu.Lock()
	isL := n.isLeader
	n.mu.Unlock()

	if !isL {
		return // Only leader replicates
	}

	cmd := ClusterCommand{
		Op:        op,
		Topic:     topic,
		WasmBytes: wasmBytes,
		DLQTopic:  dlqTopic,
		Schema:    schema,
	}
	payload, _ := json.Marshal(cmd)

	for _, peer := range n.peers {
		go func(p string) {
			conn, err := net.Dial("tcp", p)
			if err != nil {
				log.Printf("Cluster: failed to connect to peer %s", p)
				return
			}
			defer conn.Close()
			_, _ = conn.Write(payload)
		}(peer)
	}
}

func (n *RaftNode) Close() {
	if n.listener != nil {
		_ = n.listener.Close()
	}
}
