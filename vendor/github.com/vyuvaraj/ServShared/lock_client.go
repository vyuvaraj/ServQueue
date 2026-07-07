package ServShared

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"time"
)

// ──────────────────────────────────────────────────────────────────────────────
// DistributedLocker interface
//
// Any component that needs cross-service mutual exclusion depends only on this
// interface. The concrete implementation (HTTPLockClient) talks to ServMesh's
// /api/lock/* endpoints. A no-op or test double can be injected without
// touching ServMesh at all.
// ──────────────────────────────────────────────────────────────────────────────

// LockEntry mirrors the lock.LockEntry type returned by ServMesh.
type LockEntry struct {
	Key        string    `json:"key"`
	Owner      string    `json:"owner"`
	ExpiresAt  time.Time `json:"expires_at"`
	AcquiredAt time.Time `json:"acquired_at"`
}

// AcquireResult mirrors lock.AcquireResult.
type AcquireResult struct {
	Acquired bool       `json:"acquired"`
	Lock     *LockEntry `json:"lock,omitempty"`
	HeldBy   string     `json:"held_by,omitempty"`
}

// DistributedLocker is the interface all callers use.
// ServMesh's HTTPLockClient satisfies it; tests can inject a NoOpLocker.
type DistributedLocker interface {
	// Acquire tries to take the named lock for owner with the given TTL.
	// Returns (result, nil) on a successful HTTP round-trip.
	// result.Acquired==false means the lock is held by someone else.
	Acquire(key, owner string, ttl time.Duration) (AcquireResult, error)

	// Release relinquishes key if currently held by owner.
	Release(key, owner string) error

	// Extend refreshes the TTL of key for owner.
	Extend(key, owner string, ttl time.Duration) (*LockEntry, error)

	// Status returns the current lock entry for key, or nil if not held.
	Status(key string) (*LockEntry, error)
}

// ──────────────────────────────────────────────────────────────────────────────
// HTTPLockClient — production implementation backed by ServMesh
// ──────────────────────────────────────────────────────────────────────────────

// HTTPLockClient calls ServMesh's /api/lock/* endpoints over HTTP.
// It is zero-dependency from ServShared's perspective (only stdlib + JWT which
// is already in go.mod).
type HTTPLockClient struct {
	meshAddr string       // e.g. "http://localhost:8089"
	client   *http.Client
}

// NewHTTPLockClient creates a lock client pointing at the given ServMesh address.
func NewHTTPLockClient(meshAddr string) *HTTPLockClient {
	return &HTTPLockClient{
		meshAddr: meshAddr,
		client:   &http.Client{Timeout: 5 * time.Second},
	}
}

// NewHTTPLockClientFromEnv reads SERV_MESH_ADDR (falling back to
// http://localhost:8089) and returns a configured client.
func NewHTTPLockClientFromEnv() *HTTPLockClient {
	addr := getenv("SERV_MESH_ADDR", "http://localhost:8089")
	return NewHTTPLockClient(addr)
}

func (c *HTTPLockClient) Acquire(key, owner string, ttl time.Duration) (AcquireResult, error) {
	body := map[string]interface{}{
		"key":    key,
		"owner":  owner,
		"ttl_ms": ttl.Milliseconds(),
	}
	var result AcquireResult
	err := c.post("/api/lock/acquire", body, &result)
	return result, err
}

func (c *HTTPLockClient) Release(key, owner string) error {
	body := map[string]string{"key": key, "owner": owner}
	var out map[string]interface{}
	if err := c.post("/api/lock/release", body, &out); err != nil {
		return err
	}
	if released, ok := out["released"].(bool); ok && !released {
		return fmt.Errorf("lock %q not held by %q", key, owner)
	}
	return nil
}

func (c *HTTPLockClient) Extend(key, owner string, ttl time.Duration) (*LockEntry, error) {
	body := map[string]interface{}{
		"key":    key,
		"owner":  owner,
		"ttl_ms": ttl.Milliseconds(),
	}
	var entry LockEntry
	if err := c.post("/api/lock/extend", body, &entry); err != nil {
		return nil, err
	}
	return &entry, nil
}

func (c *HTTPLockClient) Status(key string) (*LockEntry, error) {
	url := fmt.Sprintf("%s/api/lock/status?key=%s", c.meshAddr, key)
	resp, err := c.client.Get(url)
	if err != nil {
		return nil, fmt.Errorf("lock status request failed: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode == http.StatusNotFound {
		return nil, nil // not held
	}
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("lock status returned HTTP %d", resp.StatusCode)
	}
	var entry LockEntry
	if err := json.NewDecoder(resp.Body).Decode(&entry); err != nil {
		return nil, err
	}
	return &entry, nil
}

func (c *HTTPLockClient) post(path string, body, out interface{}) error {
	data, err := json.Marshal(body)
	if err != nil {
		return err
	}
	resp, err := c.client.Post(c.meshAddr+path, "application/json", bytes.NewReader(data))
	if err != nil {
		return fmt.Errorf("lock request to %s failed: %w", path, err)
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 400 {
		return fmt.Errorf("lock %s returned HTTP %d", path, resp.StatusCode)
	}
	if out != nil {
		return json.NewDecoder(resp.Body).Decode(out)
	}
	return nil
}

// ──────────────────────────────────────────────────────────────────────────────
// WithLock — functional helper for mutex-style usage
//
//	err := ServShared.WithLock(locker, "billing:invoice:42", myID, 10*time.Second, func() error {
//	    return processInvoice(42)
//	})
// ──────────────────────────────────────────────────────────────────────────────

// WithLock acquires key, runs fn, then always releases the lock.
// If the lock is already held, it returns an error immediately (no retry).
// For retry-with-backoff, use WithLockRetry.
func WithLock(l DistributedLocker, key, owner string, ttl time.Duration, fn func() error) error {
	result, err := l.Acquire(key, owner, ttl)
	if err != nil {
		return fmt.Errorf("acquire %q: %w", key, err)
	}
	if !result.Acquired {
		return fmt.Errorf("lock %q already held by %q", key, result.HeldBy)
	}
	defer func() { _ = l.Release(key, owner) }()
	return fn()
}

// WithLockRetry tries to acquire the lock up to maxAttempts times, sleeping
// backoff between each attempt. Useful for contended operations.
func WithLockRetry(
	l DistributedLocker,
	key, owner string,
	ttl time.Duration,
	maxAttempts int,
	backoff time.Duration,
	fn func() error,
) error {
	for attempt := 1; attempt <= maxAttempts; attempt++ {
		result, err := l.Acquire(key, owner, ttl)
		if err != nil {
			return fmt.Errorf("acquire %q attempt %d: %w", key, attempt, err)
		}
		if result.Acquired {
			defer func() { _ = l.Release(key, owner) }()
			return fn()
		}
		if attempt < maxAttempts {
			time.Sleep(backoff)
		}
	}
	return fmt.Errorf("could not acquire lock %q after %d attempts", key, maxAttempts)
}

// ──────────────────────────────────────────────────────────────────────────────
// NoOpLocker — always grants the lock; useful in tests / single-instance mode
// ──────────────────────────────────────────────────────────────────────────────

// NoOpLocker is a DistributedLocker that always succeeds.
// Use it in single-instance deployments or tests where distributed locking is
// not needed.
type NoOpLocker struct{}

func (NoOpLocker) Acquire(key, owner string, ttl time.Duration) (AcquireResult, error) {
	now := time.Now()
	return AcquireResult{
		Acquired: true,
		Lock: &LockEntry{
			Key: key, Owner: owner,
			AcquiredAt: now,
			ExpiresAt:  now.Add(ttl),
		},
	}, nil
}
func (NoOpLocker) Release(key, owner string) error                             { return nil }
func (NoOpLocker) Extend(key, owner string, ttl time.Duration) (*LockEntry, error) {
	now := time.Now()
	return &LockEntry{Key: key, Owner: owner, ExpiresAt: now.Add(ttl)}, nil
}
func (NoOpLocker) Status(key string) (*LockEntry, error) { return nil, nil }
