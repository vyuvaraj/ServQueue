package ServShared

import (
	"crypto/rsa"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"regexp"
	"strings"
	"time"

	"github.com/golang-jwt/jwt/v5"
)

// ContextKey is the type for context keys to avoid collisions.
type ContextKey string

const (
	// ClaimsContextKey is the context key for authenticated claims.
	ClaimsContextKey ContextKey = "servverse-claims"
	// TenantContextKey is the context key for the verified tenant ID.
	TenantContextKey ContextKey = "servverse-tenant-id"
)

// GetClaims extracts claims from request context (set by AuthMiddleware).
func GetClaims(r *http.Request) *Claims {
	if c, ok := r.Context().Value(ClaimsContextKey).(*Claims); ok {
		return c
	}
	return nil
}

// GetTenantID extracts the verified tenant ID from request context (set by TenantMiddleware).
// Always use this instead of reading X-Tenant-ID directly to ensure it has been verified.
func GetTenantID(r *http.Request) string {
	if tid, ok := r.Context().Value(TenantContextKey).(string); ok {
		return tid
	}
	return ""
}

// AuthMiddleware returns an HTTP middleware that enforces JWT auth.
//
// Behavior:
//   - If SERV_JWT_SECRET is empty: all requests pass through (dev mode)
//   - If SERV_JWT_SECRET is set: requires valid Bearer JWT on all routes
//   - /healthz and /readyz are always allowed without auth
func AuthMiddleware(next http.Handler) http.Handler {
	secret := os.Getenv("SERV_JWT_SECRET")
	jwksURL := os.Getenv("SERV_JWKS_URL")
	if secret == "" && jwksURL == "" {
		return next // dev mode — no auth enforced
	}

	validator := NewAuthValidator(secret, jwksURL, "")

	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Always allow health probes without auth
		path := r.URL.Path
		if path == "/healthz" || path == "/readyz" || path == "/health" {
			next.ServeHTTP(w, r)
			return
		}

		// Extract token
		token, err := ExtractTokenFromHeader(r.Header.Get("Authorization"))
		if err != nil {
			writeAuthError(w, http.StatusUnauthorized, "Unauthorized: "+err.Error(), "ERR_MISSING_AUTH")
			return
		}

		// Validate
		claims, err := validator.ValidateToken(token)
		if err != nil {
			writeAuthError(w, http.StatusUnauthorized, "Unauthorized: "+err.Error(), "ERR_INVALID_TOKEN")
			return
		}

		// Inject claims into context
		ctx := context.WithValue(r.Context(), ClaimsContextKey, claims)
		next.ServeHTTP(w, r.WithContext(ctx))
	})
}

// TenantMiddleware enforces that the X-Tenant-ID request header matches the
// tenant_id claim embedded in the verified JWT. This prevents callers from
// impersonating tenants by forging the header.
//
// Behavior:
//   - If no JWT is present in context (auth disabled / dev mode): falls back to
//     X-Tenant-ID header value, defaulting to "default" if absent.
//   - If JWT is present and has a tenant_id claim: X-Tenant-ID MUST match.
//   - If JWT has no tenant_id claim (e.g. service tokens): X-Tenant-ID is
//     accepted as-is (service tokens are implicitly trusted for any tenant).
//   - The verified tenant ID is injected into context via TenantContextKey.
func TenantMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		headerTenant := r.Header.Get("X-Tenant-ID")
		if headerTenant == "" {
			headerTenant = "default"
		}

		claims := GetClaims(r)
		verifiedTenant := headerTenant

		if claims != nil && claims.TenantID != "" {
			// Enforce: header must match JWT claim exactly.
			if headerTenant != claims.TenantID {
				writeAuthError(w, http.StatusForbidden,
					"Forbidden: X-Tenant-ID does not match authenticated tenant",
					"ERR_TENANT_MISMATCH")
				return
			}
			verifiedTenant = claims.TenantID
		}

		// Inject verified tenant ID into context.
		ctx := context.WithValue(r.Context(), TenantContextKey, verifiedTenant)
		next.ServeHTTP(w, r.WithContext(ctx))
	})
}

// GenerateServiceToken creates a long-lived JWT for inter-service communication.
// The token identifies the calling service and has the "service" role.
func GenerateServiceToken(secret string, serviceName string) (string, error) {
	if secret == "" {
		return "", nil // dev mode — no token needed
	}

	claims := Claims{
		Username: serviceName,
		Roles:    []string{"service"},
		Scopes:   []string{"*"},
		RegisteredClaims: jwt.RegisteredClaims{
			Issuer:    "servverse",
			Subject:   serviceName,
			IssuedAt:  jwt.NewNumericDate(time.Now()),
			ExpiresAt: jwt.NewNumericDate(time.Now().Add(365 * 24 * time.Hour)),
		},
	}

	token := jwt.NewWithClaims(jwt.SigningMethodHS256, claims)
	return token.SignedString([]byte(secret))
}

// GenerateUserToken creates a JWT for a user with given roles and tenant.
func GenerateUserToken(secret string, username string, roles []string, tenantID string, ttl time.Duration) (string, error) {
	claims := Claims{
		Username: username,
		Roles:    roles,
		TenantID: tenantID,
		RegisteredClaims: jwt.RegisteredClaims{
			Issuer:    "servverse",
			Subject:   username,
			IssuedAt:  jwt.NewNumericDate(time.Now()),
			ExpiresAt: jwt.NewNumericDate(time.Now().Add(ttl)),
		},
	}

	token := jwt.NewWithClaims(jwt.SigningMethodHS256, claims)
	return token.SignedString([]byte(secret))
}

// GenerateUserTokenRS256 creates a JWT signed with an RSA private key.
func GenerateUserTokenRS256(privKey *rsa.PrivateKey, kid string, username string, roles []string, tenantID string, ttl time.Duration) (string, error) {
	claims := Claims{
		Username: username,
		Roles:    roles,
		TenantID: tenantID,
		RegisteredClaims: jwt.RegisteredClaims{
			Issuer:    "servverse",
			Subject:   username,
			IssuedAt:  jwt.NewNumericDate(time.Now()),
			ExpiresAt: jwt.NewNumericDate(time.Now().Add(ttl)),
		},
	}

	token := jwt.NewWithClaims(jwt.SigningMethodRS256, claims)
	token.Header["kid"] = kid
	return token.SignedString(privKey)
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
	InitTrace(serviceName)

	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		traceparent := r.Header.Get("traceparent")
		if traceparent == "" {
			traceparent = r.Header.Get("X-Request-ID")
		}

		span := StartSpan(fmt.Sprintf("%s %s", r.Method, r.URL.Path), traceparent)
		if span != nil {
			span.Kind = 2 // Server span
			tpVal := fmt.Sprintf("00-%s-%s-01", span.TraceID, span.SpanID)
			r.Header.Set("traceparent", tpVal)
		}

		wrapper := &responseWriterWrapper{ResponseWriter: w, statusCode: http.StatusOK}

		defer func() {
			if span != nil {
				attrs := map[string]interface{}{
					"http.method":      r.Method,
					"http.status_code": wrapper.statusCode,
					"http.url":         r.URL.String(),
				}
				var err error
				if wrapper.statusCode >= 400 {
					err = fmt.Errorf("HTTP error status %d", wrapper.statusCode)
				}
				EndSpan(span, err, attrs)
			}
		}()

		next.ServeHTTP(wrapper, r)
	})
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

// IsolateTopic prefixes a topic name with the tenant ID from context.
func IsolateTopic(ctx context.Context, topic string) string {
	if tid, ok := ctx.Value(TenantContextKey).(string); ok && tid != "" && tid != "default" {
		return tid + "-" + topic
	}
	return topic
}

// IsolateDBPool prefixes database name with the tenant ID from context.
func IsolateDBPool(ctx context.Context, dbName string) string {
	if tid, ok := ctx.Value(TenantContextKey).(string); ok && tid != "" && tid != "default" {
		return tid + "_" + dbName
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



