package main

import (
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync"
	"testing"

	"servqueue/pkg/broker"
	"servqueue/pkg/storage"
)

func TestStorageTieringOffloading(t *testing.T) {
	// Clean up any default WAL files
	_ = os.Remove("temp_test.wal")
	defer os.Remove("temp_test.wal")

	// Set up mock S3/ServStore server
	var mu sync.Mutex
	receivedRequests := make(map[string][]byte)
	receivedHeaders := make(map[string]http.Header)

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != "PUT" {
			http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
			return
		}

		body, err := io.ReadAll(r.Body)
		if err != nil {
			http.Error(w, "Failed to read body", http.StatusBadRequest)
			return
		}

		mu.Lock()
		receivedRequests[r.URL.Path] = body
		receivedHeaders[r.URL.Path] = r.Header.Clone()
		mu.Unlock()

		w.WriteHeader(http.StatusOK)
	}))
	defer ts.Close()

	// Initialize broker engine
	engine := broker.NewBrokerEngine()
	defer engine.Stop()

	// Open a custom WAL for testing
	wal, err := storage.OpenWAL("temp_test.wal")
	if err != nil {
		t.Fatalf("Failed to open WAL: %v", err)
	}
	defer wal.Close()

	// Set small maxSize to trigger rotation on next append
	wal.SetMaxSize(10)

	// Set up the offloader to point to the mock server
	engine.ConfigureOffloader(ts.URL, "test-bucket", "test-secret-token")

	var offloadWG sync.WaitGroup
	offloadWG.Add(1)

	// Wrap the OnRotate function to notify us when rotation and offload completes
	wal.OnRotate = func(closedPath string) {
		defer offloadWG.Done()
		err := engine.GetOffloader().OffloadSegment(closedPath)
		if err != nil {
			t.Errorf("OffloadSegment failed: %v", err)
		}
	}

	// First append to set bytesWrit above maxSize (10)
	err = wal.Append("my-topic", "payload-1")
	if err != nil {
		t.Fatalf("First append failed: %v", err)
	}

	// Second append to trigger the actual rotation checks
	err = wal.Append("my-topic", "payload-2")
	if err != nil {
		t.Fatalf("Second append failed: %v", err)
	}

	// Wait for the offloader to finish
	offloadWG.Wait()

	// Let's verify the mock server received the segment
	mu.Lock()
	reqCount := len(receivedRequests)
	var receivedBody []byte
	var headers http.Header
	var receivedPath string
	for path, body := range receivedRequests {
		receivedPath = path
		receivedBody = body
		headers = receivedHeaders[path]
	}
	mu.Unlock()

	if reqCount != 1 {
		t.Fatalf("Expected exactly 1 request to the mock server, got %d", reqCount)
	}

	if !strings.HasPrefix(receivedPath, "/test-bucket/wal/temp_test.wal.") {
		t.Errorf("Expected path format to be /test-bucket/wal/temp_test.wal.TIMESTAMP, got %q", receivedPath)
	}

	if len(receivedBody) == 0 {
		t.Errorf("Expected non-empty uploaded segment body")
	}

	// Verify headers (token and content-type)
	if auth := headers.Get("Authorization"); auth != "Bearer test-secret-token" {
		t.Errorf("Expected Authorization header 'Bearer test-secret-token', got %q", auth)
	}
	if ct := headers.Get("Content-Type"); ct != "application/octet-stream" {
		t.Errorf("Expected Content-Type 'application/octet-stream', got %q", ct)
	}

	// Verify local rotated file is deleted
	parts := strings.Split(receivedPath, "/")
	filename := parts[len(parts)-1]
	if _, err := os.Stat(filename); !os.IsNotExist(err) {
		t.Errorf("Expected local rotated segment file %q to be deleted, but it still exists", filename)
		_ = os.Remove(filename)
	}
}
