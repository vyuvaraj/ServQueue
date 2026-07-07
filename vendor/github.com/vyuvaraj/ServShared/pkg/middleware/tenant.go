package middleware

import (
	"context"
	"net/http"
)

const (
	// TenantContextKey is the context key for the verified tenant ID.
	TenantContextKey ContextKey = "servverse-tenant-id"
)

// GetTenantID extracts the verified tenant ID from request context (set by TenantMiddleware).
func GetTenantID(r *http.Request) string {
	if tid, ok := r.Context().Value(TenantContextKey).(string); ok {
		return tid
	}
	return ""
}

// TenantMiddleware enforces that the X-Tenant-ID request header matches the
// tenant_id claim embedded in the verified JWT.
func TenantMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		headerTenant := r.Header.Get("X-Tenant-ID")
		if headerTenant == "" {
			headerTenant = "default"
		}

		claims := GetClaims(r)
		verifiedTenant := headerTenant

		if claims != nil && claims.TenantID != "" {
			if headerTenant != claims.TenantID {
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(http.StatusForbidden)
				w.Write([]byte(`{"error":"Forbidden: X-Tenant-ID does not match authenticated tenant","code":"ERR_TENANT_MISMATCH"}`))
				return
			}
			verifiedTenant = claims.TenantID
		}

		ctx := context.WithValue(r.Context(), TenantContextKey, verifiedTenant)
		next.ServeHTTP(w, r.WithContext(ctx))
	})
}
