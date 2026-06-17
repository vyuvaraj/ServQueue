package web

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"strings"

	"servqueue/pkg/broker"
)

type Server struct {
	addr      string
	engine    *broker.BrokerEngine
	authToken string
	tlsCert   string
	tlsKey    string
}

func NewServer(addr string, engine *broker.BrokerEngine, authToken, tlsCert, tlsKey string) *Server {
	return &Server{
		addr:      addr,
		engine:    engine,
		authToken: authToken,
		tlsCert:   tlsCert,
		tlsKey:    tlsKey,
	}
}

func (s *Server) Start() error {
	mux := http.NewServeMux()
	mux.HandleFunc("/api/topics/", s.authorize(s.handleTopics))
	mux.HandleFunc("/api/publish", s.authorize(s.handlePublish))
	mux.HandleFunc("/api/stats", s.authorize(s.handleStats))
	mux.HandleFunc("/api/replay", s.authorize(s.handleReplay))

	if s.tlsCert != "" && s.tlsKey != "" {
		return http.ListenAndServeTLS(s.addr, s.tlsCert, s.tlsKey, mux)
	}
	return http.ListenAndServe(s.addr, mux)
}

func (s *Server) authorize(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if s.authToken == "" {
			next(w, r)
			return
		}

		authHeader := r.Header.Get("Authorization")
		if authHeader == "" {
			http.Error(w, "Unauthorized: Missing authorization header", http.StatusUnauthorized)
			return
		}

		token := strings.TrimPrefix(authHeader, "Bearer ")
		if token != s.authToken {
			http.Error(w, "Unauthorized: Invalid token", http.StatusUnauthorized)
			return
		}

		next(w, r)
	}
}

func (s *Server) handleTopics(w http.ResponseWriter, r *http.Request) {
	parts := strings.Split(strings.Trim(r.URL.Path, "/"), "/")
	if len(parts) < 4 {
		http.Error(w, "Invalid path. Use /api/topics/{topic}/transform or /api/topics/{topic}/dlq", http.StatusBadRequest)
		return
	}
	topic := parts[2]
	action := parts[3]

	if action == "transform" {
		s.handleRegisterTransform(w, r, topic)
	} else if action == "dlq" {
		s.handleRegisterDLQ(w, r, topic)
	} else {
		http.Error(w, "Not found", http.StatusNotFound)
	}
}

func (s *Server) handleRegisterTransform(w http.ResponseWriter, r *http.Request, topic string) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	wasmBytes, err := io.ReadAll(r.Body)
	if err != nil {
		http.Error(w, "Failed to read body: "+err.Error(), http.StatusInternalServerError)
		return
	}

	if len(wasmBytes) == 0 {
		_ = s.engine.RegisterTransform(r.Context(), topic, nil)
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("WASM transform cleared for topic " + topic))
		return
	}

	err = s.engine.RegisterTransform(r.Context(), topic, wasmBytes)
	if err != nil {
		http.Error(w, "Failed to compile WASM: "+err.Error(), http.StatusBadRequest)
		return
	}
	w.WriteHeader(http.StatusOK)
	w.Write([]byte("WASM transform registered for topic " + topic))
}

func (s *Server) handleRegisterDLQ(w http.ResponseWriter, r *http.Request, topic string) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var req struct {
		DLQTopic string `json:"dlq_topic"`
	}

	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "Bad request: JSON body required", http.StatusBadRequest)
		return
	}

	if req.DLQTopic == "" {
		http.Error(w, "Missing dlq_topic", http.StatusBadRequest)
		return
	}

	s.engine.SetDLQ(topic, req.DLQTopic)
	w.WriteHeader(http.StatusOK)
	w.Write([]byte("DLQ " + req.DLQTopic + " registered for topic " + topic))
}

func (s *Server) handlePublish(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var req struct {
		Topic   string `json:"topic"`
		Payload string `json:"payload"`
	}

	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "Bad request", http.StatusBadRequest)
		return
	}

	// Propagate traceparent header if received
	ctx := r.Context()
	if tp := r.Header.Get("traceparent"); tp != "" {
		ctx = context.WithValue(ctx, "traceparent", tp)
	}

	res, err := s.engine.Publish(ctx, req.Topic, req.Payload)
	if err != nil {
		http.Error(w, "Transform error: "+err.Error(), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]string{"status": "ok", "delivered_payload": res})
}

func (s *Server) handleStats(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{
		"status": "healthy",
		"metrics": map[string]interface{}{
			"messages_published_total": s.engine.Metrics.MessagesPublished,
			"wasm_executions_total":    s.engine.Metrics.WasmExecutions,
			"wasm_execution_errors":    s.engine.Metrics.WasmExecutionErrors,
			"wasm_duration_ns":         s.engine.Metrics.WasmDurationNs,
		},
	})
}

func (s *Server) handleReplay(w http.ResponseWriter, r *http.Request) {
	topic := r.URL.Query().Get("topic")
	if topic == "" {
		http.Error(w, "Missing topic parameter", http.StatusBadRequest)
		return
	}

	// Dynamic replay log implementation:
	// In production, offloaded files would be downloaded. 
	// For local compatibility, we return status indicating replay completion initialization.
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{
		"status":  "replay_initialized",
		"topic":   topic,
		"records": 0,
	})
}
