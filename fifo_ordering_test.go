package main

import (
	"context"
	"fmt"
	"os"
	"sort"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"servqueue/pkg/broker"
)

// TestFIFOOrderingWithinPartition verifies that messages published sequentially
// to a topic by a single producer arrive to a subscriber in the same order.
func TestFIFOOrderingWithinPartition(t *testing.T) {
	_ = os.Remove("queue.wal")
	defer os.Remove("queue.wal")

	engine := broker.NewBrokerEngine()
	defer engine.Stop()

	topic := "fifo-single"
	const N = 100
	subChan := engine.Subscribe(topic)
	defer engine.Unsubscribe(topic, subChan)

	for i := range N {
		msg := fmt.Sprintf("msg-%05d", i)
		if _, err := engine.Publish(context.Background(), topic, msg); err != nil {
			t.Fatalf("Publish %d: %v", i, err)
		}
	}

	received := make([]string, 0, N)
	timeout := time.After(5 * time.Second)
	for len(received) < N {
		select {
		case msg := <-subChan:
			received = append(received, msg)
		case <-timeout:
			t.Fatalf("timed out after receiving %d/%d messages", len(received), N)
		}
	}

	// Verify FIFO order
	for i, msg := range received {
		expected := fmt.Sprintf("msg-%05d", i)
		if msg != expected {
			t.Errorf("position %d: got %q, want %q", i, msg, expected)
		}
	}
}

// TestFIFOOrderingConcurrentPublishers verifies that even with concurrent
// publishers, each producer's messages arrive in the order they were sent.
// (Cross-producer ordering is not guaranteed — only per-producer FIFO is tested.)
func TestFIFOOrderingConcurrentPublishers(t *testing.T) {
	_ = os.Remove("queue.wal")
	defer os.Remove("queue.wal")

	engine := broker.NewBrokerEngine()
	defer engine.Stop()

	topic := "fifo-concurrent"
	const producers = 4
	const msgsPerProducer = 50
	const totalMsgs = producers * msgsPerProducer

	subChan := engine.Subscribe(topic)
	defer engine.Unsubscribe(topic, subChan)

	// Track per-producer last sequence seen
	lastSeen := make([]int64, producers)
	for i := range lastSeen {
		lastSeen[i] = -1
	}

	var wg sync.WaitGroup
	for p := range producers {
		wg.Add(1)
		go func(producerID int) {
			defer wg.Done()
			for seq := range msgsPerProducer {
				// Encode producerID and sequence into the message
				msg := fmt.Sprintf("p%d:seq%05d", producerID, seq)
				if _, err := engine.Publish(context.Background(), topic, msg); err != nil {
					// Rate limiter may reject; retry once
					time.Sleep(5 * time.Millisecond)
					_, _ = engine.Publish(context.Background(), topic, msg)
				}
			}
		}(p)
	}
	wg.Wait()

	// Drain with a generous timeout
	received := make([]string, 0, totalMsgs)
	timeout := time.After(10 * time.Second)
drain:
	for len(received) < totalMsgs {
		select {
		case msg := <-subChan:
			received = append(received, msg)
		case <-timeout:
			break drain // partial drain — still check what we got
		}
	}

	t.Logf("received %d/%d messages", len(received), totalMsgs)

	// Verify per-producer FIFO ordering
	for _, msg := range received {
		var producerID, seq int
		if _, err := fmt.Sscanf(msg, "p%d:seq%05d", &producerID, &seq); err != nil {
			t.Errorf("unexpected message format: %q", msg)
			continue
		}
		if int64(seq) <= lastSeen[producerID] {
			t.Errorf("out-of-order message from producer %d: got seq %d, last seen %d",
				producerID, seq, lastSeen[producerID])
		}
		lastSeen[producerID] = int64(seq)
	}
}

// TestFIFOOrderingHighVolume stress-tests FIFO ordering with 1000 messages
// from a single producer to confirm no reordering under load.
func TestFIFOOrderingHighVolume(t *testing.T) {
	_ = os.Remove("queue.wal")
	defer os.Remove("queue.wal")

	engine := broker.NewBrokerEngine()
	defer engine.Stop()

	topic := "fifo-highvol"
	const N = 1000
	subChan := engine.Subscribe(topic)
	defer engine.Unsubscribe(topic, subChan)

	var published atomic.Int64
	for i := range N {
		msg := fmt.Sprintf("%08d", i)
		if _, err := engine.Publish(context.Background(), topic, msg); err == nil {
			published.Add(1)
		}
	}
	total := int(published.Load())
	t.Logf("published %d/%d messages (rate limiter may have dropped some)", total, N)

	received := make([]string, 0, total)
	// Drain with a sliding idle-window: stop when no new message arrives for 500ms.
	idle := time.NewTimer(500 * time.Millisecond)
	defer idle.Stop()
drain:
	for {
		select {
		case msg := <-subChan:
			received = append(received, msg)
			if !idle.Stop() {
				select {
				case <-idle.C:
				default:
				}
			}
			idle.Reset(500 * time.Millisecond)
		case <-idle.C:
			break drain
		}
	}
	t.Logf("received %d/%d messages", len(received), total)


	// Messages must be in strictly ascending numeric order
	if !sort.SliceIsSorted(received, func(i, j int) bool {
		return received[i] < received[j]
	}) {
		t.Error("messages are not in FIFO order")
		// Report first out-of-order pair
		for i := 1; i < len(received); i++ {
			if received[i] < received[i-1] {
				t.Errorf("  inversion at index %d: %q < %q", i, received[i], received[i-1])
				break
			}
		}
	}
}

// BenchmarkFIFOThroughput measures messages/sec for a single producer.
func BenchmarkFIFOThroughput(b *testing.B) {
	_ = os.Remove("queue.wal")
	defer os.Remove("queue.wal")

	engine := broker.NewBrokerEngine()
	defer engine.Stop()

	topic := "bench-fifo"
	subChan := engine.Subscribe(topic)
	defer engine.Unsubscribe(topic, subChan)

	// Drain goroutine to keep the channel from backing up
	done := make(chan struct{})
	go func() {
		defer close(done)
		for range subChan {
		}
	}()

	ctx := context.Background()
	b.ReportAllocs()
	for b.Loop() {
		_, _ = engine.Publish(ctx, topic, "bench-payload")
	}
	engine.Unsubscribe(topic, subChan)
	<-done
}
