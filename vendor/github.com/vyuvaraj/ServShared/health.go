package ServShared

import (
	"encoding/json"
	"net/http"
	"sync"
)

// HealthCheck is a function that returns an error if the check fails.
type HealthCheck func() error

type HealthRegistry struct {
	mu     sync.RWMutex
	checks map[string]HealthCheck
}

var defaultRegistry = &HealthRegistry{
	checks: make(map[string]HealthCheck),
}

// RegisterCheck registers a named health check function.
func RegisterCheck(name string, check HealthCheck) {
	defaultRegistry.Register(name, check)
}

// Register registers a named health check function.
func (r *HealthRegistry) Register(name string, check HealthCheck) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.checks[name] = check
}

// CheckRuns all registered health checks.
func (r *HealthRegistry) Check() map[string]string {
	r.mu.RLock()
	defer r.mu.RUnlock()

	results := make(map[string]string)
	for name, check := range r.checks {
		if err := check(); err != nil {
			results[name] = err.Error()
		} else {
			results[name] = "OK"
		}
	}
	return results
}

// HealthzHandler returns 200 OK if the service is running.
func HealthzHandler(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte(`{"status":"healthy"}`))
}

// ReadyzHandler executes all registered health checks and returns 200 if OK, 503 if any check fails.
func ReadyzHandler(w http.ResponseWriter, r *http.Request) {
	ReadyzHandlerWithRegistry(defaultRegistry, w, r)
}

// ReadyzHandlerWithRegistry runs checks from a custom registry and handles HTTP response.
func ReadyzHandlerWithRegistry(registry *HealthRegistry, w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	results := registry.Check()

	hasFailures := false
	for _, status := range results {
		if status != "OK" {
			hasFailures = true
			break
		}
	}

	response := map[string]interface{}{
		"status": "ready",
		"checks": results,
	}

	if hasFailures {
		response["status"] = "unready"
		w.WriteHeader(http.StatusServiceUnavailable)
	} else {
		w.WriteHeader(http.StatusOK)
	}

	body, _ := json.Marshal(response)
	_, _ = w.Write(body)
}
