package broker

import (
	"context"
	"os"
	"runtime"
	"strings"
	"testing"
)

// TestBackpressureMemoryBound verifies that when backpressure limit is reached,
// the publisher gets rejected with a capacity error, and memory consumption remains bounded.
func TestBackpressureMemoryBound(t *testing.T) {
	// Set queue limit to a small value for testing backpressure
	os.Setenv("SERVQUEUE_BACKPRESSURE_LIMIT", "10")
	defer os.Unsetenv("SERVQUEUE_BACKPRESSURE_LIMIT")

	// Set publish rate limiter high or disable it to not interfere
	os.Setenv("SERVQUEUE_PUBLISH_RATE", "100000")
	os.Setenv("SERVQUEUE_PUBLISH_CAPACITY", "100000")
	defer os.Unsetenv("SERVQUEUE_PUBLISH_RATE")
	defer os.Unsetenv("SERVQUEUE_PUBLISH_CAPACITY")

	engine := NewBrokerEngine()
	defer engine.Stop()

	topic := "backpressure-topic"

	// 1. Fill the queue up to limit (10 items)
	ctx := context.Background()
	for i := 1; i <= 10; i++ {
		_, err := engine.Publish(ctx, topic, "message-body-payload")
		if err != nil {
			t.Fatalf("failed to publish baseline message %d: %v", i, err)
		}
	}

	// 2. Measure memory before triggering backpressure rejections
	runtime.GC()
	var ms1 runtime.MemStats
	runtime.ReadMemStats(&ms1)

	// 3. Attempt to publish 1,000 more messages, expecting all to fail due to backpressure
	rejectedCount := 0
	for i := 0; i < 1000; i++ {
		_, err := engine.Publish(ctx, topic, "message-body-payload")
		if err != nil && strings.Contains(err.Error(), "queue capacity exceeded") {
			rejectedCount++
		}
	}

	if rejectedCount != 1000 {
		t.Errorf("expected 1000 rejected messages, got %d", rejectedCount)
	}

	// 4. Measure memory after rejections
	runtime.GC()
	var ms2 runtime.MemStats
	runtime.ReadMemStats(&ms2)

	// Since rejected messages are discarded, heap allocation should remain completely flat/bounded
	deltaHeap := int64(ms2.HeapAlloc) - int64(ms1.HeapAlloc)
	t.Logf("HeapAlloc before: %d KB, after: %d KB, delta: %d KB", ms1.HeapAlloc/1024, ms2.HeapAlloc/1024, deltaHeap/1024)

	// Bounded allocation threshold: allow very minimal metadata/gc slack (e.g. < 500 KB)
	if deltaHeap > 500*1024 {
		t.Errorf("memory grew by %d KB, indicating non-bounded memory usage during backpressure rejections", deltaHeap/1024)
	}
}
