package ServShared

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"regexp"
	"strings"
	"time"

	"github.com/vyuvaraj/ServShared/pkg/middleware"
)

// ContextKey is the type for context keys to avoid collisions.
type ContextKey string

const (
	// ClaimsContextKey is the context key for authenticated claims.
	ClaimsContextKey ContextKey = ContextKey(middleware.ClaimsContextKey)
	// TenantContextKey is the context key for the verified tenant ID.
	TenantContextKey ContextKey = ContextKey(middleware.TenantContextKey)
)

// GetClaims extracts claims from request context (set by AuthMiddleware).
func GetClaims(r *http.Request) *Claims {
	mc := middleware.GetClaims(r)
	if mc == nil {
		return nil
	}
	// Map middleware.Claims to ServShared.Claims
	return &Claims{
		Username: mc.Username,
		Roles:    mc.Roles,
		Scopes:   mc.Scopes,
		TenantID: mc.TenantID,
	}
}

// GetTenantID extracts the verified tenant ID from request context (set by TenantMiddleware).
// Always use this instead of reading X-Tenant-ID directly to ensure it has been verified.
func GetTenantID(r *http.Request) string {
	return middleware.GetTenantID(r)
}

// AuthMiddleware returns an HTTP middleware that enforces JWT auth.
func AuthMiddleware(next http.Handler) http.Handler {
	return middleware.AuthMiddleware(next)
}

// TenantMiddleware enforces that the X-Tenant-ID request header matches the
// tenant_id claim embedded in the verified JWT.
func TenantMiddleware(next http.Handler) http.Handler {
	return middleware.TenantMiddleware(next)
}



// HasRole checks if the authenticated claims include a specific role.
func HasRole(r *http.Request, role string) bool {
	claims := GetClaims(r)
	if claims == nil {
		return false
	}
	for _, r := range claims.Roles {
		if r == role || r == "admin" {
			return true
		}
	}
	return false
}

func writeAuthError(w http.ResponseWriter, status int, msg, code string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(map[string]string{
		"error": msg,
		"code":  code,
	})
}

// responseWriterWrapper wraps http.ResponseWriter to capture status code
type responseWriterWrapper struct {
	http.ResponseWriter
	statusCode int
}

func (w *responseWriterWrapper) WriteHeader(code int) {
	w.statusCode = code
	w.ResponseWriter.WriteHeader(code)
}

// TraceMiddleware intercepts requests to create OTel spans and propagates trace headers
func TraceMiddleware(serviceName string, next http.Handler) http.Handler {
	return middleware.TraceMiddleware(serviceName, next)
}

// LogJSON logs a message in structured JSON format, extracting trace ID from request context if available
func LogJSON(r *http.Request, level, msg string) {
	entry := map[string]interface{}{
		"level": level,
		"ts":    time.Now().Format(time.RFC3339),
		"msg":   SanitizeLog(msg),
	}
	if r != nil {
		entry["method"] = r.Method
		entry["path"] = r.URL.Path
		tp := r.Header.Get("traceparent")
		if tp != "" {
			parts := strings.Split(tp, "-")
			if len(parts) >= 3 {
				entry["trace_id"] = parts[1]
			}
		}
	}
	data, _ := json.Marshal(entry)
	fmt.Println(string(data))
}

// Validatable represents a request payload that can validate its own fields.
type Validatable interface {
	Validate() error
}

// DecodeAndValidateJSON decodes JSON from a request body and validates it.
// It returns true on success, or false if it wrote an HTTP error response.
func DecodeAndValidateJSON(w http.ResponseWriter, r *http.Request, dest Validatable) bool {
	if err := json.NewDecoder(r.Body).Decode(dest); err != nil {
		writeAuthError(w, http.StatusBadRequest, "Invalid JSON payload: "+err.Error(), "ERR_BAD_REQUEST")
		return false
	}
	if err := dest.Validate(); err != nil {
		writeAuthError(w, http.StatusBadRequest, "Validation failed: "+err.Error(), "ERR_VALIDATION_FAILED")
		return false
	}
	return true
}

// MaxBytesMiddleware limits the size of the request body.
func MaxBytesMiddleware(limit int64) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			r.Body = http.MaxBytesReader(w, r.Body, limit)
			next.ServeHTTP(w, r)
		})
	}
}

var sanitizeRegex = regexp.MustCompile(`(?i)(["']?)(password|secret|token|key|authorization|bearer|passwd)(["']?)\s*[:=]\s*("([^"]+)"|'([^']+)'|([^\s,"'\r\n]+))`)

// SanitizeLog redacts sensitive information from log messages.
func SanitizeLog(msg string) string {
	return sanitizeRegex.ReplaceAllStringFunc(msg, func(match string) string {
		sepIdx := strings.IndexAny(match, ":=")
		if sepIdx == -1 {
			return match
		}
		keyPart := match[:sepIdx+1]
		valPart := match[sepIdx+1:]
		trimmedVal := strings.TrimSpace(valPart)
		
		spaceLen := len(valPart) - len(strings.TrimLeft(valPart, " \t"))
		leadingSpaces := valPart[:spaceLen]

		if strings.HasPrefix(trimmedVal, "\"") && strings.HasSuffix(trimmedVal, "\"") {
			return keyPart + leadingSpaces + `"[REDACTED]"`
		}
		if strings.HasPrefix(trimmedVal, "'") && strings.HasSuffix(trimmedVal, "'") {
			return keyPart + leadingSpaces + `'[REDACTED]'`
		}
		return keyPart + leadingSpaces + "[REDACTED]"
	})
}

var (
	// IsolateTopicHook is dynamically injected by the Enterprise Edition module.
	IsolateTopicHook func(ctx context.Context, topic string) string
	// IsolateDBPoolHook is dynamically injected by the Enterprise Edition module.
	IsolateDBPoolHook func(ctx context.Context, dbName string) string
)

// IsolateTopic prefixes a topic name with the tenant ID from context.
// In OSS: returns the topic unchanged (single-tenant mode).
// In EE: prefixes with tenant ID for multi-tenant isolation.
func IsolateTopic(ctx context.Context, topic string) string {
	if IsolateTopicHook != nil {
		return IsolateTopicHook(ctx, topic)
	}
	return topic
}

// IsolateDBPool prefixes database name with the tenant ID from context.
// In OSS: returns the dbName unchanged (single-tenant mode).
// In EE: prefixes with tenant ID for multi-tenant isolation.
func IsolateDBPool(ctx context.Context, dbName string) string {
	if IsolateDBPoolHook != nil {
		return IsolateDBPoolHook(ctx, dbName)
	}
	return dbName
}

// VersionHandler returns a JSON version response.
func VersionHandler(serviceName, version string) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		json.NewEncoder(w).Encode(map[string]string{
			"service": serviceName,
			"version": version,
			"edition": Edition,
		})
	}
}

// DeprecationMiddleware adds Deprecation and Sunset headers to response.
func DeprecationMiddleware(sunsetDate string) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Deprecation", "true")
			if sunsetDate != "" {
				w.Header().Set("Sunset", sunsetDate)
			}
			next.ServeHTTP(w, r)
		})
	}
}



