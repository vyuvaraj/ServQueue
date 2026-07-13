package broker

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"testing"
)

func runThroughputBench(b *testing.B, size int) {
	_ = os.Remove("queue.wal")
	defer os.Remove("queue.wal")

	// Set publish rate limiter high or disable it to not interfere
	os.Setenv("SERVQUEUE_PUBLISH_RATE", "1000000")
	os.Setenv("SERVQUEUE_PUBLISH_CAPACITY", "1000000")
	defer os.Unsetenv("SERVQUEUE_PUBLISH_RATE")
	defer os.Unsetenv("SERVQUEUE_PUBLISH_CAPACITY")

	engine := NewBrokerEngine()
	defer engine.Stop()

	topic := fmt.Sprintf("throughput-topic-%d", size)
	subChan := engine.Subscribe(topic)
	defer engine.Unsubscribe(topic, subChan)

	// Consume channel in background to prevent queue blocking/backpressure
	done := make(chan struct{})
	go func() {
		defer close(done)
		for range subChan {
		}
	}()

	payload := bytes.Repeat([]byte("x"), size)
	payloadStr := string(payload)

	ctx := context.Background()
	b.SetBytes(int64(size))
	b.ReportAllocs()
	b.ResetTimer()

	for b.Loop() {
		_, err := engine.Publish(ctx, topic, payloadStr)
		if err != nil {
			b.Fatalf("failed to publish: %v", err)
		}
	}

	engine.Unsubscribe(topic, subChan)
	<-done
}

func BenchmarkThroughput1KB(b *testing.B) {
	runThroughputBench(b, 1024)
}

func BenchmarkThroughput64KB(b *testing.B) {
	runThroughputBench(b, 64*1024)
}

func BenchmarkThroughput1MB(b *testing.B) {
	runThroughputBench(b, 1024*1024)
}
