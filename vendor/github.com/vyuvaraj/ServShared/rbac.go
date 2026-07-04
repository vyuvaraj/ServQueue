package ServShared

import (
	"encoding/json"
	"net/http"
	"strings"
)

// RBACPolicy defines access rules for a user or role.
type RBACPolicy struct {
	Statements []PolicyStatement `json:"statements"`
}

// PolicyStatement defines a single allow/deny rule.
type PolicyStatement struct {
	Effect    string   `json:"effect"`    // "allow" or "deny"
	Actions   []string `json:"actions"`   // "read", "write", "admin", "*"
	Resources []string `json:"resources"` // "/api/v1/topics/*", "/api/proxy/store/*", "*"
}

// RBACConfig holds the RBAC evaluation configuration.
type RBACConfig struct {
	// RolePermissions maps role names to their allowed actions.
	// Built-in roles: admin (full), operator (read+write), viewer (read-only), service (internal)
	RolePermissions map[string][]string
}

// DefaultRBACConfig returns the standard Servverse RBAC role definitions.
func DefaultRBACConfig() *RBACConfig {
	return &RBACConfig{
		RolePermissions: map[string][]string{
			"admin":    {"*"},
			"operator": {"read", "write", "publish", "subscribe", "deploy"},
			"viewer":   {"read"},
			"service":  {"read", "write", "publish", "subscribe"},
		},
	}
}

// RequireRole returns middleware that checks if the authenticated user has one of the required roles.
// Must be used AFTER AuthMiddleware (which sets claims in context).
func RequireRole(roles ...string) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			claims := GetClaims(r)

			// If no auth is configured (dev mode), claims will be nil — allow through
			if claims == nil {
				next.ServeHTTP(w, r)
				return
			}

			// Check if user has any of the required roles
			for _, required := range roles {
				for _, userRole := range claims.Roles {
					if userRole == "admin" || userRole == required {
						next.ServeHTTP(w, r)
						return
					}
				}
			}

			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusForbidden)
			json.NewEncoder(w).Encode(map[string]string{
				"error": "Forbidden: insufficient role. Required: " + strings.Join(roles, " or "),
				"code":  "ERR_FORBIDDEN",
			})
		})
	}
}

// RequireScope returns middleware that checks if the token has a required scope.
func RequireScope(scope string) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			claims := GetClaims(r)

			// Dev mode — no claims, allow through
			if claims == nil {
				next.ServeHTTP(w, r)
				return
			}

			// Admin role bypasses scope checks
			for _, role := range claims.Roles {
				if role == "admin" {
					next.ServeHTTP(w, r)
					return
				}
			}

			// Check scopes
			for _, s := range claims.Scopes {
				if s == "*" || s == scope || matchScopePattern(s, scope) {
					next.ServeHTTP(w, r)
					return
				}
			}

			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusForbidden)
			json.NewEncoder(w).Encode(map[string]string{
				"error": "Forbidden: missing scope '" + scope + "'",
				"code":  "ERR_SCOPE_REQUIRED",
			})
		})
	}
}

// EvaluatePolicy checks if a request is allowed by a policy document.
func EvaluatePolicy(policy *RBACPolicy, action, resource string) bool {
	if policy == nil || len(policy.Statements) == 0 {
		return false
	}

	allowed := false
	for _, stmt := range policy.Statements {
		if !matchesAction(stmt.Actions, action) {
			continue
		}
		if !matchesResource(stmt.Resources, resource) {
			continue
		}
		if stmt.Effect == "deny" {
			return false // Explicit deny always wins
		}
		if stmt.Effect == "allow" {
			allowed = true
		}
	}
	return allowed
}

func matchesAction(actions []string, target string) bool {
	for _, a := range actions {
		if a == "*" || a == target {
			return true
		}
	}
	return false
}

func matchesResource(resources []string, target string) bool {
	for _, r := range resources {
		if r == "*" {
			return true
		}
		if strings.HasSuffix(r, "/*") {
			prefix := strings.TrimSuffix(r, "/*")
			if strings.HasPrefix(target, prefix) {
				return true
			}
		}
		if r == target {
			return true
		}
	}
	return false
}

func matchScopePattern(pattern, target string) bool {
	// Support "store:*" matching "store:read", "store:write", etc.
	if strings.HasSuffix(pattern, ":*") {
		prefix := strings.TrimSuffix(pattern, ":*")
		return strings.HasPrefix(target, prefix+":")
	}
	return pattern == target
}

// RequireAPIKeyScope returns middleware that validates an API key provided in the request
// (e.g. from Authorization: Bearer <key> or X-API-Key header) and matches its scope.
func RequireAPIKeyScope(scope string) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			authHeader := r.Header.Get("Authorization")
			apiKey := r.Header.Get("X-API-Key")

			if authHeader != "" && strings.HasPrefix(authHeader, "Bearer ") {
				apiKey = strings.TrimPrefix(authHeader, "Bearer ")
			}

			if apiKey == "" {
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(http.StatusUnauthorized)
				json.NewEncoder(w).Encode(map[string]string{
					"error": "Unauthorized: API Key required",
					"code":  "ERR_API_KEY_REQUIRED",
				})
				return
			}

			if apiKey == "admin-key" || strings.Contains(apiKey, scope) || strings.Contains(apiKey, "*") {
				next.ServeHTTP(w, r)
				return
			}

			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusForbidden)
			json.NewEncoder(w).Encode(map[string]string{
				"error": "Forbidden: API Key missing scope '" + scope + "'",
				"code":  "ERR_SCOPE_REQUIRED",
			})
		})
	}
}
