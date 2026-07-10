package middleware

import (
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"net/http"
	"strings"
)

type responseWriterWrapper struct {
	http.ResponseWriter
	statusCode int
}

func (w *responseWriterWrapper) WriteHeader(code int) {
	w.statusCode = code
	w.ResponseWriter.WriteHeader(code)
}

func genID(length int) string {
	b := make([]byte, length)
	_, _ = rand.Read(b)
	return hex.EncodeToString(b)
}

// TraceMiddleware intercepts requests to create OTel trace contexts.
func TraceMiddleware(serviceName string, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		traceparent := r.Header.Get("traceparent")
		if traceparent == "" {
			traceparent = r.Header.Get("X-Request-ID")
		}

		traceID := ""
		if traceparent != "" {
			parts := strings.Split(traceparent, "-")
			if len(parts) >= 3 {
				traceID = parts[1]
			}
		}

		if traceID == "" || len(traceID) != 32 {
			traceID = genID(16)
		}
		newSpanID := genID(8)

		tpVal := fmt.Sprintf("00-%s-%s-01", traceID, newSpanID)
		r.Header.Set("traceparent", tpVal)
		w.Header().Set("traceparent", tpVal)

		wrapper := &responseWriterWrapper{ResponseWriter: w, statusCode: http.StatusOK}
		next.ServeHTTP(wrapper, r)
	})
}
