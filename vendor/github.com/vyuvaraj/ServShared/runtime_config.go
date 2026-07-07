package ServShared

import (
	"os"
	"strconv"
	"time"
)

// RuntimeConfig holds all configuration for a ServRuntime instance.
// Values are loaded from environment variables with sensible defaults.
//
// Environment variables:
//
//	SERV_MESH_ADDR      — ServMesh registry address (default: http://localhost:8089)
//	SERV_SELF_ADDR      — This service's own HTTP address (e.g. http://localhost:8080)
//	SERV_HEALTH_PATH    — Health probe path registered with ServMesh (default: /healthz)
//	SERV_HEARTBEAT_TTL  — Heartbeat interval in seconds (default: 5)
//	SERV_MAX_RETRIES    — Max retries for outbound calls (default: 3)
//	SERV_TIMEOUT_MS     — Outbound call timeout in ms (default: 2000)
//	SERV_BACKOFF_MS     — Initial backoff between retries in ms (default: 50)
//	SERV_OTEL_ENABLED   — Enable OTel tracing (default: true)
//	SERV_REGION         — Optional region tag for geo-aware routing
type RuntimeConfig struct {
	MeshAddr     string
	SelfAddr     string
	HealthPath   string
	HeartbeatTTL time.Duration
	MaxRetries   int
	TimeoutMs    int
	BackoffMs    int
	EnableOtel   bool
	Region       string
}

// DefaultRuntimeConfig returns a RuntimeConfig populated from environment
// variables, falling back to safe defaults when vars are absent.
func DefaultRuntimeConfig() *RuntimeConfig {
	return &RuntimeConfig{
		MeshAddr:     getenv("SERV_MESH_ADDR", "http://localhost:8089"),
		SelfAddr:     getenv("SERV_SELF_ADDR", ""),
		HealthPath:   getenv("SERV_HEALTH_PATH", "/healthz"),
		HeartbeatTTL: time.Duration(getenvInt("SERV_HEARTBEAT_TTL", 5)) * time.Second,
		MaxRetries:   getenvInt("SERV_MAX_RETRIES", 3),
		TimeoutMs:    getenvInt("SERV_TIMEOUT_MS", 2000),
		BackoffMs:    getenvInt("SERV_BACKOFF_MS", 50),
		EnableOtel:   getenvBool("SERV_OTEL_ENABLED", true),
		Region:       getenv("SERV_REGION", ""),
	}
}

// Option is a functional option for configuring a RuntimeConfig.
type Option func(*RuntimeConfig)

// WithMeshAddr overrides the ServMesh registry address.
func WithMeshAddr(addr string) Option { return func(c *RuntimeConfig) { c.MeshAddr = addr } }

// WithSelfAddr sets this service's own advertised address.
func WithSelfAddr(addr string) Option { return func(c *RuntimeConfig) { c.SelfAddr = addr } }

// WithHealthPath overrides the health probe path.
func WithHealthPath(path string) Option { return func(c *RuntimeConfig) { c.HealthPath = path } }

// WithHeartbeatTTL overrides the heartbeat interval.
func WithHeartbeatTTL(d time.Duration) Option {
	return func(c *RuntimeConfig) { c.HeartbeatTTL = d }
}

// WithMaxRetries overrides the max retry count for outbound calls.
func WithMaxRetries(n int) Option { return func(c *RuntimeConfig) { c.MaxRetries = n } }

// WithRegion sets the geo-region tag for routing.
func WithRegion(region string) Option { return func(c *RuntimeConfig) { c.Region = region } }

// WithOtel enables or disables OTel tracing.
func WithOtel(enabled bool) Option { return func(c *RuntimeConfig) { c.EnableOtel = enabled } }

// --- helpers ---------------------------------------------------------------

func getenv(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func getenvInt(key string, def int) int {
	if v := os.Getenv(key); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			return n
		}
	}
	return def
}

func getenvBool(key string, def bool) bool {
	if v := os.Getenv(key); v != "" {
		if b, err := strconv.ParseBool(v); err == nil {
			return b
		}
	}
	return def
}
