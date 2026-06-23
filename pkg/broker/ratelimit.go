package broker

import (
	"sync"
	"time"
)

// TokenBucket implements a thread-safe token bucket rate limiter.
type TokenBucket struct {
	mu         sync.Mutex
	rate       float64 // tokens refilled per second
	capacity   float64 // max burst capacity
	tokens     float64 // current token count
	lastRefill time.Time
}

// NewTokenBucket creates a new TokenBucket with the given refill rate and capacity.
func NewTokenBucket(rate float64, capacity float64) *TokenBucket {
	return &TokenBucket{
		rate:       rate,
		capacity:   capacity,
		tokens:     capacity,
		lastRefill: time.Now(),
	}
}

// Allow returns true if a token is available and consumed, false otherwise.
func (tb *TokenBucket) Allow() bool {
	tb.mu.Lock()
	defer tb.mu.Unlock()

	now := time.Now()
	elapsed := now.Sub(tb.lastRefill).Seconds()
	tb.lastRefill = now

	tb.tokens += elapsed * tb.rate
	if tb.tokens > tb.capacity {
		tb.tokens = tb.capacity
	}

	if tb.tokens >= 1.0 {
		tb.tokens -= 1.0
		return true
	}

	return false
}
