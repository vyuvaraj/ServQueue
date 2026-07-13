package broker

import (
	"context"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// TestExactlyOnceDedupAccuracy publishes the same message ID 1,000 times concurrently
// and verifies that exactly 1 delivery occurs to the subscriber and 999 are rejected as duplicates.
func TestExactlyOnceDedupAccuracy(t *testing.T) {
	_ = os.Remove("queue.wal")
	defer os.Remove("queue.wal")

	// Set publish rate limiter high or disable it to not interfere
	os.Setenv("SERVQUEUE_PUBLISH_RATE", "100000")
	os.Setenv("SERVQUEUE_PUBLISH_CAPACITY", "100000")
	defer os.Unsetenv("SERVQUEUE_PUBLISH_RATE")
	defer os.Unsetenv("SERVQUEUE_PUBLISH_CAPACITY")

	engine := NewBrokerEngine()
	defer engine.Stop()

	topic := "dedup-accuracy-topic"
	subChan := engine.Subscribe(topic)
	defer engine.Unsubscribe(topic, subChan)

	const totalAttempts = 1000
	const concurrency = 50
	const msgID = "unique-msg-uuid-exactly-once"
	const payload = "valuable-transaction-payload"

	// Context with message-id value
	ctx := context.WithValue(context.Background(), "message-id", msgID)

	var successCount int64
	var duplicateErrorCount int64
	var otherErrorCount int64
	var wg sync.WaitGroup

	ch := make(chan struct{}, totalAttempts)
	for i := 0; i < totalAttempts; i++ {
		ch <- struct{}{}
	}
	close(ch)

	wg.Add(concurrency)
	for i := 0; i < concurrency; i++ {
		go func() {
			defer wg.Done()
			for range ch {
				_, err := engine.Publish(ctx, topic, payload)
				if err != nil {
					if strings.Contains(err.Error(), "duplicate message detected") {
						atomic.AddInt64(&duplicateErrorCount, 1)
					} else {
						atomic.AddInt64(&otherErrorCount, 1)
					}
				} else {
					atomic.AddInt64(&successCount, 1)
				}
			}
		}()
	}
	wg.Wait()

	// Drain messages from the subscriber channel
	receivedCount := 0
	timeout := time.After(500 * time.Millisecond)

drain:
	for {
		select {
		case msg := <-subChan:
			if msg == payload {
				receivedCount++
			}
		case <-timeout:
			break drain
		}
	}

	if successCount != 1 {
		t.Errorf("expected exactly 1 successful Publish call, got %d", successCount)
	}
	if duplicateErrorCount != 999 {
		t.Errorf("expected exactly 999 duplicate rejections, got %d", duplicateErrorCount)
	}
	if otherErrorCount > 0 {
		t.Errorf("got %d unexpected other errors", otherErrorCount)
	}
	if receivedCount != 1 {
		t.Errorf("expected exactly 1 received message by subscriber, got %d", receivedCount)
	}
}
