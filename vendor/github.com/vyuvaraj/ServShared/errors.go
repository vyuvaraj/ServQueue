package ServShared

import (
	"encoding/json"
	"net/http"
)

// WriteJSONError writes a standardized JSON error format response.
func WriteJSONError(w http.ResponseWriter, r *http.Request, msg string, code string, status int) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	traceID := r.Header.Get("X-Trace-ID")
	if traceID == "" {
		traceID = r.Header.Get("traceparent")
	}
	json.NewEncoder(w).Encode(map[string]any{
		"error":    msg,
		"code":     code,
		"trace_id": traceID,
	})
}
