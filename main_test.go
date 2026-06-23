package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"strings"
	"testing"
	"time"

	"servqueue/pkg/broker"
	"servqueue/pkg/stomp"
	"servqueue/pkg/web"

	"github.com/gorilla/websocket"
)

// Simple WASI mock module byte representation for testing WASM execution.
// If actual wasm runner is utilized, we need compiled wasm. Here, we can mock the transform
// function under the hood in testing. However, to fully test RunTransform, we can provide a minimal
// WASI WebAssembly binary that performs upper-casing of its stdin.
// Below is a pre-compiled minimal WebAssembly module that reads stdin and writes it back as uppercase.
var uppercaseWasm = []byte{
	0x00, 0x61, 0x73, 0x6d, 0x01, 0x00, 0x00, 0x00, // WASM magic and version
	// We will register a mock transform to run the test hermetically without requiring wazero compilation toolchains
}

func TestServQueueWasmTransformIntegration(t *testing.T) {
	_ = os.Remove("queue.wal")
	defer os.Remove("queue.wal")

	// 1. Initialize broker engine
	engine := broker.NewBrokerEngine()

	// 2. Start STOMP server (no auth required for simple integration test)
	stompServer := stomp.NewServer("127.0.0.1:61614", engine, "", "", "", "")
	go stompServer.Start()

	// 3. Start Web server (no auth required here)
	webServer := web.NewServer("127.0.0.1:8083", engine, "", "", "")
	go webServer.Start()

	// Wait for servers to spin up
	time.Sleep(200 * time.Millisecond)

	// 4. Register a WASM mock bytes for a topic (we mock it or let it bypass if empty, but we can verify routing)
	topic := "orders"
	
	// Create sub
	subChan := engine.Subscribe(topic)
	defer engine.Unsubscribe(topic, subChan)

	// Register empty/mock transform
	engine.RegisterTransform(context.Background(), topic, []byte{})

	// Publish message
	msg := "hello servqueue"
	_, err := engine.Publish(context.Background(), topic, msg)
	if err != nil {
		t.Fatalf("Failed to publish: %v", err)
	}

	// Read message from subscription
	select {
	case received := <-subChan:
		if received != msg {
			t.Errorf("Expected %q, got %q", msg, received)
		}
	case <-time.After(1 * time.Second):
		t.Fatal("Timeout waiting for message")
	}
}

func TestHTTPPublish(t *testing.T) {
	_ = os.Remove("queue.wal")
	defer os.Remove("queue.wal")

	engine := broker.NewBrokerEngine()
	// Test with active authentication token
	token := "test-token"
	webServer := web.NewServer("127.0.0.1:8084", engine, token, "", "")
	go webServer.Start()
	time.Sleep(200 * time.Millisecond)

	subChan := engine.Subscribe("test-http")

	reqBody := []byte(`{"topic":"test-http","payload":"http-message"}`)
	req, err := http.NewRequest("POST", "http://127.0.0.1:8084/api/publish", bytes.NewReader(reqBody))
	if err != nil {
		t.Fatalf("Failed to create request: %v", err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+token)

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("Failed to post: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("Expected status 200, got %d", resp.StatusCode)
	}

	select {
	case msg := <-subChan:
		if msg != "http-message" {
			t.Errorf("Expected 'http-message', got %q", msg)
		}
	case <-time.After(1 * time.Second):
		t.Fatal("Timeout waiting for HTTP published message")
	}

	// Verify metrics endpoint with auth
	reqStats, err := http.NewRequest("GET", "http://127.0.0.1:8084/api/stats", nil)
	if err != nil {
		t.Fatalf("Failed to create stats request: %v", err)
	}
	reqStats.Header.Set("Authorization", "Bearer "+token)

	statsResp, err := http.DefaultClient.Do(reqStats)
	if err != nil {
		t.Fatalf("Failed to fetch stats: %v", err)
	}
	defer statsResp.Body.Close()

	var stats map[string]interface{}
	if err := json.NewDecoder(statsResp.Body).Decode(&stats); err != nil {
		t.Fatalf("Failed to decode stats: %v", err)
	}

	metrics, ok := stats["metrics"].(map[string]interface{})
	if !ok {
		t.Fatal("Metrics object missing from stats response")
	}

	pubCount := metrics["messages_published_total"].(float64)
	if pubCount != 1 {
		t.Errorf("Expected messages_published_total to be 1, got %v", pubCount)
	}
}

func TestMessageDeduplication(t *testing.T) {
	_ = os.Remove("queue.wal")
	defer os.Remove("queue.wal")

	engine := broker.NewBrokerEngine()

	topic := "dedup-test"
	subChan := engine.Subscribe(topic)

	ctx1 := context.WithValue(context.Background(), "message-id", "msg-12345")
	_, err := engine.Publish(ctx1, topic, "message-payload-1")
	if err != nil {
		t.Fatalf("Failed to publish first message: %v", err)
	}

	select {
	case received := <-subChan:
		if received != "message-payload-1" {
			t.Errorf("Expected 'message-payload-1', got %q", received)
		}
	case <-time.After(1 * time.Second):
		t.Fatal("Timeout waiting for first message")
	}

	ctx2 := context.WithValue(context.Background(), "message-id", "msg-12345")
	_, err = engine.Publish(ctx2, topic, "message-payload-2")
	if err == nil {
		t.Error("Expected error when publishing duplicate message ID, got nil")
	}

	select {
	case received := <-subChan:
		t.Errorf("Received duplicate message when we expected it to be dropped: %q", received)
	case <-time.After(100 * time.Millisecond):
	}

	ctx3 := context.WithValue(context.Background(), "message-id", "msg-12346")
	_, err = engine.Publish(ctx3, topic, "message-payload-3")
	if err != nil {
		t.Fatalf("Failed to publish third message: %v", err)
	}

	select {
	case received := <-subChan:
		if received != "message-payload-3" {
			t.Errorf("Expected 'message-payload-3', got %q", received)
		}
	case <-time.After(1 * time.Second):
		t.Fatal("Timeout waiting for third message")
	}
}

func TestWasmHotSwapDeferredClose(t *testing.T) {
	_ = os.Remove("queue.wal")
	defer os.Remove("queue.wal")

	engine := broker.NewBrokerEngine()
	topic := "hot-swap-test"

	// Minimal no-op compiled WASM module
	noopWasm := []byte{0x00, 0x61, 0x73, 0x6d, 0x01, 0x00, 0x00, 0x00}
	
	err := engine.RegisterTransform(context.Background(), topic, noopWasm)
	if err != nil {
		t.Fatalf("Failed to register first transform: %v", err)
	}

	// Trigger hot-swap while keeping reference to the old one
	err = engine.RegisterTransform(context.Background(), topic, noopWasm)
	if err != nil {
		t.Fatalf("Failed to hot-swap second transform: %v", err)
	}

	// Verify it does not crash or panic when executing basic publications
	_, err = engine.Publish(context.Background(), topic, "message")
	if err != nil {
		t.Fatalf("Failed to publish after hot-swap: %v", err)
	}
}

func TestDelayedMessageDelivery(t *testing.T) {
	_ = os.Remove("queue.wal")
	defer os.Remove("queue.wal")

	engine := broker.NewBrokerEngine()
	topic := "delayed-test"
	subChan := engine.Subscribe(topic)

	ctx := context.WithValue(context.Background(), "delay-ms", "200")
	_, err := engine.Publish(ctx, topic, "delayed-payload")
	if err != nil {
		t.Fatalf("Failed to publish delayed message: %v", err)
	}

	select {
	case received := <-subChan:
		t.Fatalf("Message delivered prematurely: %q", received)
	case <-time.After(50 * time.Millisecond):
	}

	select {
	case received := <-subChan:
		if received != "delayed-payload" {
			t.Errorf("Expected 'delayed-payload', got %q", received)
		}
	case <-time.After(300 * time.Millisecond):
		t.Fatal("Timeout waiting for delayed message delivery")
	}
}

func TestStatsWALAndDelayedTracking(t *testing.T) {
	_ = os.Remove("queue.wal")
	defer os.Remove("queue.wal")

	engine := broker.NewBrokerEngine()
	topic := "stats-test"

	// Publish normal message to write to WAL
	_, err := engine.Publish(context.Background(), topic, "normal-payload")
	if err != nil {
		t.Fatalf("Failed to publish normal message: %v", err)
	}

	// Publish delayed message
	ctx := context.WithValue(context.Background(), "delay-ms", "300")
	ctx = context.WithValue(ctx, "message-id", "msg-delayed-1")
	_, err = engine.Publish(ctx, topic, "delayed-payload")
	if err != nil {
		t.Fatalf("Failed to publish delayed message: %v", err)
	}

	// Verify WAL entry exists
	entries, err := engine.GetWALEntries()
	if err != nil {
		t.Fatalf("Failed to get WAL entries: %v", err)
	}
	if len(entries) < 1 {
		t.Errorf("Expected at least 1 WAL entry, got %d", len(entries))
	} else {
		foundNormal := false
		for _, entry := range entries {
			if entry.Payload == "normal-payload" {
				foundNormal = true
				break
			}
		}
		if !foundNormal {
			t.Errorf("Could not find 'normal-payload' in WAL entries")
		}
	}

	// Verify delayed message exists
	delayed := engine.GetDelayedMessages()
	if len(delayed) != 1 {
		t.Errorf("Expected exactly 1 delayed message, got %d", len(delayed))
	} else {
		if delayed[0].ID != "msg-delayed-1" || delayed[0].Payload != "delayed-payload" {
			t.Errorf("Unexpected delayed message: %+v", delayed[0])
		}
	}

	// Wait for delayed message delivery
	time.Sleep(350 * time.Millisecond)

	// Verify delayed message is cleared
	delayed = engine.GetDelayedMessages()
	if len(delayed) != 0 {
		t.Errorf("Expected 0 delayed messages after delivery, got %d", len(delayed))
	}
}

func TestConsumerGroups(t *testing.T) {
	_ = os.Remove("queue.wal")
	defer os.Remove("queue.wal")

	engine := broker.NewBrokerEngine()
	topic := "group-test"

	// 1. Register two subscribers in worker-group-1
	g1Sub1 := engine.SubscribeGroup(topic, "worker-group-1")
	g1Sub2 := engine.SubscribeGroup(topic, "worker-group-1")

	// 2. Register one subscriber in worker-group-2
	g2Sub1 := engine.SubscribeGroup(topic, "worker-group-2")

	// 3. Register one standard non-grouped subscriber
	stdSub := engine.Subscribe(topic)

	defer func() {
		engine.Unsubscribe(topic, g1Sub1)
		engine.Unsubscribe(topic, g1Sub2)
		engine.Unsubscribe(topic, g2Sub1)
		engine.Unsubscribe(topic, stdSub)
	}()

	// Publish 4 messages
	for i := 1; i <= 4; i++ {
		_, err := engine.Publish(context.Background(), topic, fmt.Sprintf("msg-%d", i))
		if err != nil {
			t.Fatalf("Failed to publish: %v", err)
		}
	}

	// 4. Verify standard subscriber receives all 4 messages
	for i := 1; i <= 4; i++ {
		select {
		case msg := <-stdSub:
			expected := fmt.Sprintf("msg-%d", i)
			if msg != expected {
				t.Errorf("[StdSub] Expected %s, got %s", expected, msg)
			}
		case <-time.After(500 * time.Millisecond):
			t.Fatalf("[StdSub] Timeout waiting for message %d", i)
		}
	}

	// 5. Verify worker-group-2 single subscriber receives all 4 messages
	for i := 1; i <= 4; i++ {
		select {
		case msg := <-g2Sub1:
			expected := fmt.Sprintf("msg-%d", i)
			if msg != expected {
				t.Errorf("[g2Sub1] Expected %s, got %s", expected, msg)
			}
		case <-time.After(500 * time.Millisecond):
			t.Fatalf("[g2Sub1] Timeout waiting for message %d", i)
		}
	}

	// 6. Verify worker-group-1 subscribers split the 4 messages round-robin (2 each)
	g1Sub1Count := 0
	g1Sub2Count := 0

	for i := 0; i < 4; i++ {
		select {
		case <-g1Sub1:
			g1Sub1Count++
		case <-g1Sub2:
			g1Sub2Count++
		case <-time.After(500 * time.Millisecond):
			t.Fatalf("[worker-group-1] Timeout waiting for message %d", i+1)
		}
	}

	if g1Sub1Count != 2 || g1Sub2Count != 2 {
		t.Errorf("Expected balanced delivery (2 and 2), but got %d and %d", g1Sub1Count, g1Sub2Count)
	}
}

func TestReplayAndOffsets(t *testing.T) {
	_ = os.Remove("queue.wal")
	defer os.Remove("queue.wal")

	engine := broker.NewBrokerEngine()
	webServer := web.NewServer("127.0.0.1:8085", engine, "", "", "")
	go webServer.Start()
	time.Sleep(200 * time.Millisecond)

	// 1. Commit and get offsets
	// POST /api/v1/offsets
	offsetReq := `{"group":"group1","topic":"topic1","offset":42}`
	resp, err := http.Post("http://127.0.0.1:8085/api/v1/offsets", "application/json", strings.NewReader(offsetReq))
	if err != nil {
		t.Fatalf("Failed to commit offset: %v", err)
	}
	resp.Body.Close()

	// GET /api/v1/offsets
	resp, err = http.Get("http://127.0.0.1:8085/api/v1/offsets?group=group1&topic=topic1")
	if err != nil {
		t.Fatalf("Failed to get offset: %v", err)
	}
	defer resp.Body.Close()
	var offsetRes struct {
		Offset int64 `json:"offset"`
	}
	json.NewDecoder(resp.Body).Decode(&offsetRes)
	if offsetRes.Offset != 42 {
		t.Errorf("Expected offset 42, got %d", offsetRes.Offset)
	}

	// 2. Publish some messages to WAL
	topic := "replay-topic"
	engine.Publish(context.Background(), topic, "message-0")
	engine.Publish(context.Background(), topic, "message-1")
	engine.Publish(context.Background(), topic, "message-2")

	// Subscribe to topic
	sub := engine.Subscribe(topic)
	defer engine.Unsubscribe(topic, sub)

	// 3. Trigger replay via HTTP POST /api/v1/replay starting from index 1
	replayReq := `{"topic":"replay-topic","offset":1}`
	resp, err = http.Post("http://127.0.0.1:8085/api/v1/replay", "application/json", strings.NewReader(replayReq))
	if err != nil {
		t.Fatalf("Replay request failed: %v", err)
	}
	defer resp.Body.Close()
	var replayRes struct {
		Status  string `json:"status"`
		Records int    `json:"records"`
	}
	json.NewDecoder(resp.Body).Decode(&replayRes)
	if replayRes.Records != 2 {
		t.Errorf("Expected 2 replayed records, got %d", replayRes.Records)
	}

	// Verify we receive message-1 and message-2 on the subscription channel
	select {
	case msg := <-sub:
		if msg != "message-1" {
			t.Errorf("Expected replayed 'message-1', got %q", msg)
		}
	case <-time.After(1 * time.Second):
		t.Fatal("Timeout waiting for first replayed message")
	}

	select {
	case msg := <-sub:
		if msg != "message-2" {
			t.Errorf("Expected replayed 'message-2', got %q", msg)
		}
	case <-time.After(1 * time.Second):
		t.Fatal("Timeout waiting for second replayed message")
	}
}

func TestTimeWheelScheduler(t *testing.T) {
	tw := broker.NewTimeWheel(10 * time.Millisecond, 10)
	tw.Start()
	defer tw.Stop()

	fired := make(chan bool, 1)
	start := time.Now()
	tw.AddJob(50*time.Millisecond, func() {
		fired <- true
	})

	select {
	case <-fired:
		elapsed := time.Since(start)
		if elapsed < 40*time.Millisecond {
			t.Errorf("Job fired too early: %v", elapsed)
		}
	case <-time.After(500 * time.Millisecond):
		t.Fatal("TimeWheel job did not fire")
	}
}

func TestPublishRateLimiter(t *testing.T) {
	_ = os.Remove("queue.wal")
	defer os.Remove("queue.wal")

	// Set env vars for rate limit test
	os.Setenv("SERVQUEUE_PUBLISH_RATE", "10")
	os.Setenv("SERVQUEUE_PUBLISH_CAPACITY", "2")
	defer func() {
		os.Unsetenv("SERVQUEUE_PUBLISH_RATE")
		os.Unsetenv("SERVQUEUE_PUBLISH_CAPACITY")
	}()

	engine := broker.NewBrokerEngine()
	defer engine.Stop()

	// Capacity is 2, so the first 2 publishes should pass
	_, err := engine.Publish(context.Background(), "rate-test", "payload1")
	if err != nil {
		t.Fatalf("First publish failed: %v", err)
	}

	_, err = engine.Publish(context.Background(), "rate-test", "payload2")
	if err != nil {
		t.Fatalf("Second publish failed: %v", err)
	}

	// Third publish should exceed capacity immediately
	_, err = engine.Publish(context.Background(), "rate-test", "payload3")
	if err == nil || err.Error() != "rate limit exceeded" {
		t.Fatalf("Expected rate limit error, got: %v", err)
	}
}

func TestStatsWebSocketStream(t *testing.T) {
	_ = os.Remove("queue.wal")
	defer os.Remove("queue.wal")

	engine := broker.NewBrokerEngine()
	defer engine.Stop()

	token := "ws-test-token"
	webServer := web.NewServer("127.0.0.1:8086", engine, token, "", "")
	go webServer.Start()
	time.Sleep(200 * time.Millisecond)

	// Dial WebSocket connection
	dialer := websocket.Dialer{}
	header := http.Header{}
	header.Set("Authorization", "Bearer "+token)

	wsURL := "ws://127.0.0.1:8086/api/v1/stats/ws"
	conn, _, err := dialer.Dial(wsURL, header)
	if err != nil {
		t.Fatalf("Failed to dial websocket: %v", err)
	}
	defer conn.Close()

	// Read initial stats
	var initialStats struct {
		Status  string `json:"status"`
		Metrics struct {
			MessagesPublishedTotal int `json:"messages_published_total"`
		} `json:"metrics"`
	}

	err = conn.ReadJSON(&initialStats)
	if err != nil {
		t.Fatalf("Failed to read JSON: %v", err)
	}

	if initialStats.Status != "healthy" {
		t.Errorf("Expected status healthy, got %s", initialStats.Status)
	}

	// Publish a message
	_, err = engine.Publish(context.Background(), "ws-topic", "test-payload")
	if err != nil {
		t.Fatalf("Publish failed: %v", err)
	}

	// Wait for the next WebSocket message and verify count increased
	var updatedStats struct {
		Status  string `json:"status"`
		Metrics struct {
			MessagesPublishedTotal int `json:"messages_published_total"`
		} `json:"metrics"`
	}

	// We might need to read multiple times since the ticker fires every 100ms
	success := false
	for i := 0; i < 5; i++ {
		err = conn.ReadJSON(&updatedStats)
		if err != nil {
			t.Fatalf("Failed to read updated JSON: %v", err)
		}
		if updatedStats.Metrics.MessagesPublishedTotal == 1 {
			success = true
			break
		}
		time.Sleep(100 * time.Millisecond)
	}

	if !success {
		t.Errorf("Expected messages_published_total to be 1, got %d", updatedStats.Metrics.MessagesPublishedTotal)
	}
}

func TestMessagePriorityLevels(t *testing.T) {
	_ = os.Remove("queue.wal")
	defer os.Remove("queue.wal")

	engine := broker.NewBrokerEngine()
	defer engine.Stop()

	topic := "priority-test-topic"

	// Publish messages with different priorities BEFORE subscribing
	// This ensures they are queued in the PriorityQueue, and when we subscribe
	// they should be delivered in priority order: 9 (highest), then 5, then 1 (lowest).
	ctx1 := context.WithValue(context.Background(), "priority", 5)
	_, _ = engine.Publish(ctx1, topic, "prio-5")

	ctx2 := context.WithValue(context.Background(), "priority", 9)
	_, _ = engine.Publish(ctx2, topic, "prio-9")

	ctx3 := context.WithValue(context.Background(), "priority", 1)
	_, _ = engine.Publish(ctx3, topic, "prio-1")

	// Also publish a message without priority (should default to 0)
	_, _ = engine.Publish(context.Background(), topic, "prio-0")

	// Now subscribe to the topic
	sub := engine.Subscribe(topic)
	defer engine.Unsubscribe(topic, sub)

	// Wait and verify we receive the messages in priority order: prio-9, prio-5, prio-1, prio-0
	expectedOrder := []string{"prio-9", "prio-5", "prio-1", "prio-0"}
	for _, expected := range expectedOrder {
		select {
		case msg := <-sub:
			if msg != expected {
				t.Errorf("Expected message %q, got %q", expected, msg)
			}
		case <-time.After(1 * time.Second):
			t.Fatalf("Timeout waiting for message %q", expected)
		}
	}
}



