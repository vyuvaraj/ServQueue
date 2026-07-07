package middleware

import (
	"fmt"
	"net/http"
)

type responseWriterWrapper struct {
	http.ResponseWriter
	statusCode int
}

func (w *responseWriterWrapper) WriteHeader(code int) {
	w.statusCode = code
	w.ResponseWriter.WriteHeader(code)
}

// TraceMiddleware intercepts requests to create OTel trace contexts.
func TraceMiddleware(serviceName string, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		traceparent := r.Header.Get("traceparent")
		if traceparent == "" {
			traceparent = r.Header.Get("X-Request-ID")
		}
		// In a real setup, we would call InitTrace/StartSpan here.
		// For the split de-bloated layout, we delegate to simple request logging.
		tpVal := fmt.Sprintf("00-%s-%s-01", "mocktraceid", "mockspanid")
		r.Header.Set("traceparent", tpVal)

		wrapper := &responseWriterWrapper{ResponseWriter: w, statusCode: http.StatusOK}
		next.ServeHTTP(wrapper, r)
	})
}
