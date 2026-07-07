package ServShared

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"sync"
	"time"
)

// MeshResolver is the interface ServRuntime uses to communicate with ServMesh.
// The concrete implementation (MeshTransport / MeshClient) lives in ServMesh
// to avoid circular dependencies. Wire it at service boot time.
type MeshResolver interface {
	// Register registers this service instance with the mesh.
	Register(ctx context.Context, service, address, healthURL string) error
	// Heartbeat refreshes the TTL for a registered instance.
	Heartbeat(ctx context.Context, service, address string) error
}

// ServRuntime is the unified host agent that every Servverse service should
// embed. A single call to Start() replaces the manual wiring of mesh
// registration, heartbeat loops, and OTel initialisation that would otherwise
// live in each service's main.go.
//
// Usage:
//
//	rt := ServShared.NewRuntime("my-service",
//	    ServShared.WithSelfAddr("http://localhost:8080"),
//	    ServShared.WithMeshAddr("http://localhost:8089"),
//	)
//	// Wire the concrete mesh client (lives in ServMesh/pkg/client)
//	rt.SetResolver(meshclient.NewMeshTransport(meshAddr, 5*time.Second))
//	if err := rt.Start(ctx); err != nil { log.Fatal(err) }
//	defer rt.Stop()
type ServRuntime struct {
	ServiceName string
	Config      *RuntimeConfig

	mu       sync.Mutex
	resolver MeshResolver
	cancel   context.CancelFunc
	done     chan struct{}
}

// NewRuntime creates a new ServRuntime for the given service name.
// Options are applied on top of DefaultRuntimeConfig().
func NewRuntime(serviceName string, opts ...Option) *ServRuntime {
	cfg := DefaultRuntimeConfig()
	for _, opt := range opts {
		opt(cfg)
	}
	return &ServRuntime{
		ServiceName: serviceName,
		Config:      cfg,
		done:        make(chan struct{}),
	}
}

// SetResolver injects the concrete MeshResolver (e.g. *client.MeshTransport).
// Must be called before Start() when mesh integration is desired.
// If no resolver is set, Start() skips registration/heartbeat silently.
func (r *ServRuntime) SetResolver(res MeshResolver) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.resolver = res
}

// Start boots the runtime:
//  1. Initialises OTel tracing (if enabled).
//  2. Registers this service instance with ServMesh (if resolver + SelfAddr set).
//  3. Launches the background heartbeat loop.
//
// Start is non-blocking after the initial registration attempt.
func (r *ServRuntime) Start(ctx context.Context) error {
	// 1. OTel init
	if r.Config.EnableOtel {
		InitTrace(r.ServiceName)
	}

	// 2. Initial registration
	r.mu.Lock()
	resolver := r.resolver
	r.mu.Unlock()

	healthURL := ""
	if r.Config.SelfAddr != "" {
		healthURL = r.Config.SelfAddr + r.Config.HealthPath
	}

	if resolver != nil && r.Config.SelfAddr != "" {
		if err := resolver.Register(ctx, r.ServiceName, r.Config.SelfAddr, healthURL); err != nil {
			return fmt.Errorf("servruntime: mesh registration failed for %q: %w", r.ServiceName, err)
		}
	}

	// 3. Heartbeat loop
	runCtx, cancel := context.WithCancel(context.Background())
	r.cancel = cancel

	go r.heartbeatLoop(runCtx, resolver)

	return nil
}

// Stop cancels the heartbeat loop and shuts down OTel tracing.
func (r *ServRuntime) Stop() {
	r.mu.Lock()
	cancel := r.cancel
	r.mu.Unlock()

	if cancel != nil {
		cancel()
	}
	<-r.done
}

// heartbeatLoop sends periodic heartbeats to ServMesh until ctx is cancelled.
func (r *ServRuntime) heartbeatLoop(ctx context.Context, resolver MeshResolver) {
	defer close(r.done)

	if resolver == nil || r.Config.SelfAddr == "" {
		return
	}

	ticker := time.NewTicker(r.Config.HeartbeatTTL)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			// Best-effort: log on failure but don't crash.
			if err := resolver.Heartbeat(ctx, r.ServiceName, r.Config.SelfAddr); err != nil {
				LogJSON(nil, "warn", fmt.Sprintf("servruntime: heartbeat failed for %q: %v", r.ServiceName, err))
			}
		}
	}
}

// --- Simple HTTP mesh resolver for wiring without ServMesh import -----------

// httpMeshResolver is a lightweight MeshResolver that speaks directly to the
// ServMesh REST API. Services that import only ServShared (not ServMesh) can
// use this as the resolver — no ServMesh pkg dependency required.
type httpMeshResolver struct {
	baseURL    string
	httpClient *http.Client
}

// NewHTTPMeshResolver returns a MeshResolver backed by plain HTTP calls to
// the ServMesh registry REST API at meshAddr (e.g. "http://localhost:8089").
// This is the zero-dependency alternative to servmesh/pkg/client.MeshTransport.
func NewHTTPMeshResolver(meshAddr string) MeshResolver {
	return &httpMeshResolver{
		baseURL: meshAddr,
		httpClient: &http.Client{
			Timeout: 5 * time.Second,
		},
	}
}

func (h *httpMeshResolver) Register(ctx context.Context, service, address, healthURL string) error {
	payload := map[string]string{
		"service":    service,
		"address":    address,
		"health_url": healthURL,
	}
	return h.post(ctx, "/api/register", payload)
}

func (h *httpMeshResolver) Heartbeat(ctx context.Context, service, address string) error {
	payload := map[string]string{
		"service": service,
		"address": address,
	}
	return h.post(ctx, "/api/heartbeat", payload)
}

func (h *httpMeshResolver) post(ctx context.Context, path string, body interface{}) error {
	data, err := json.Marshal(body)
	if err != nil {
		return err
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost,
		h.baseURL+path, bytes.NewReader(data))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := h.httpClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	io.Copy(io.Discard, resp.Body) //nolint:errcheck

	if resp.StatusCode >= 300 {
		return fmt.Errorf("mesh API %s returned HTTP %d", path, resp.StatusCode)
	}
	return nil
}
