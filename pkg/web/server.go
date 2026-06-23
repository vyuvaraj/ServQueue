package web

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"io"
	"log"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"

	"servqueue/pkg/broker"
	"servqueue/pkg/storage"

	"github.com/gorilla/websocket"
	"github.com/vyuvaraj/ServShared"
)

type Server struct {
	addr      string
	engine    *broker.BrokerEngine
	authToken string
	tlsCert   string
	tlsKey    string
	httpSrv   *http.Server
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
	mux.HandleFunc("/healthz", ServShared.HealthzHandler)
	mux.HandleFunc("/readyz", ServShared.ReadyzHandler)
	mux.HandleFunc("/api/topics/", s.authorize(s.handleTopics))
	mux.HandleFunc("/api/v1/topics/", s.authorize(s.handleTopics))
	mux.HandleFunc("/api/topics", s.authorize(s.handleListTopics))
	mux.HandleFunc("/api/v1/topics", s.authorize(s.handleListTopics))
	mux.HandleFunc("/api/publish", s.authorize(s.handlePublish))
	mux.HandleFunc("/api/v1/publish", s.authorize(s.handlePublish))
	mux.HandleFunc("/api/stats", s.authorize(s.handleStats))
	mux.HandleFunc("/api/v1/stats", s.authorize(s.handleStats))
	mux.HandleFunc("/api/replay", s.authorize(s.handleReplay))
	mux.HandleFunc("/api/v1/replay", s.authorize(s.handleReplay))
	mux.HandleFunc("/api/offsets", s.authorize(s.handleOffsets))
	mux.HandleFunc("/api/v1/offsets", s.authorize(s.handleOffsets))
	mux.HandleFunc("/api/stats/ws", s.authorize(s.handleStatsWS))
	mux.HandleFunc("/api/v1/stats/ws", s.authorize(s.handleStatsWS))

	s.httpSrv = &http.Server{
		Addr:    s.addr,
		Handler: mux,
	}

	if s.tlsCert != "" && s.tlsKey != "" {
		return s.httpSrv.ListenAndServeTLS(s.tlsCert, s.tlsKey)
	}
	return s.httpSrv.ListenAndServe()
}

func (s *Server) Shutdown(ctx context.Context) error {
	if s.httpSrv != nil {
		return s.httpSrv.Shutdown(ctx)
	}
	return nil
}

func (s *Server) authorize(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if s.authToken == "" {
			next(w, r)
			return
		}

		authHeader := r.Header.Get("Authorization")
		if authHeader == "" {
			WriteJSONError(w, r, "Unauthorized: Missing authorization header", "ERR_MISSING_AUTH_HEADER", http.StatusUnauthorized)
			return
		}

		token := strings.TrimPrefix(authHeader, "Bearer ")
		
		authenticated := false
		if token == s.authToken {
			authenticated = true
		} else if jwtSec := os.Getenv("SERV_JWT_SECRET"); jwtSec != "" {
			if _, ok := validateJWT(token, []byte(jwtSec)); ok {
				authenticated = true
			}
		}

		if !authenticated {
			WriteJSONError(w, r, "Unauthorized: Invalid token", "ERR_INVALID_TOKEN", http.StatusUnauthorized)
			return
		}

		next(w, r)
	}
}

func (s *Server) handleListTopics(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		WriteJSONError(w, r, "Method not allowed", "ERR_METHOD_NOT_ALLOWED", http.StatusMethodNotAllowed)
		return
	}
	topics := s.engine.ListTopics()
	if topics == nil {
		topics = []broker.TopicInfo{}
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{
		"topics": topics,
		"count":  len(topics),
	})
}

func (s *Server) handleTopics(w http.ResponseWriter, r *http.Request) {
	parts := strings.Split(strings.Trim(r.URL.Path, "/"), "/")
	var topic, action string
	if len(parts) >= 5 && parts[1] == "v1" {
		topic = parts[3]
		action = parts[4]
	} else if len(parts) >= 4 {
		topic = parts[2]
		action = parts[3]
	} else {
		WriteJSONError(w, r, "Invalid path. Use /api/v1/topics/{topic}/transform or /api/v1/topics/{topic}/dlq", "ERR_INVALID_PATH", http.StatusBadRequest)
		return
	}

	switch action {
	case "transform":
		s.handleRegisterTransform(w, r, topic)
	case "dlq":
		s.handleRegisterDLQ(w, r, topic)
	case "schema":
		s.handleRegisterSchema(w, r, topic)
	default:
		WriteJSONError(w, r, "Not found", "ERR_NOT_FOUND", http.StatusNotFound)
	}
}

func (s *Server) handleRegisterSchema(w http.ResponseWriter, r *http.Request, topic string) {
	if r.Method != http.MethodPost {
		WriteJSONError(w, r, "Method not allowed", "ERR_METHOD_NOT_ALLOWED", http.StatusMethodNotAllowed)
		return
	}

	var schema map[string]string
	if err := json.NewDecoder(r.Body).Decode(&schema); err != nil {
		WriteJSONError(w, r, "Bad request: invalid schema JSON payload", "ERR_BAD_REQUEST_BODY", http.StatusBadRequest)
		return
	}

	s.engine.RegisterSchema(topic, schema)
	w.WriteHeader(http.StatusOK)
	w.Write([]byte("Schema registered for topic " + topic))
}

func (s *Server) handleRegisterTransform(w http.ResponseWriter, r *http.Request, topic string) {
	if r.Method != http.MethodPost {
		WriteJSONError(w, r, "Method not allowed", "ERR_METHOD_NOT_ALLOWED", http.StatusMethodNotAllowed)
		return
	}

	wasmBytes, err := io.ReadAll(r.Body)
	if err != nil {
		WriteJSONError(w, r, "Failed to read body: "+err.Error(), "ERR_INTERNAL_SERVER_ERROR", http.StatusInternalServerError)
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
		WriteJSONError(w, r, "Failed to compile WASM: "+err.Error(), "ERR_WASM_COMPILATION_FAILED", http.StatusBadRequest)
		return
	}
	w.WriteHeader(http.StatusOK)
	w.Write([]byte("WASM transform registered for topic " + topic))
}

func (s *Server) handleRegisterDLQ(w http.ResponseWriter, r *http.Request, topic string) {
	if r.Method != http.MethodPost {
		WriteJSONError(w, r, "Method not allowed", "ERR_METHOD_NOT_ALLOWED", http.StatusMethodNotAllowed)
		return
	}

	var req struct {
		DLQTopic string `json:"dlq_topic"`
	}

	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		WriteJSONError(w, r, "Bad request: JSON body required", "ERR_BAD_REQUEST_BODY", http.StatusBadRequest)
		return
	}

	if req.DLQTopic == "" {
		WriteJSONError(w, r, "Missing dlq_topic", "ERR_MISSING_DLQ_TOPIC", http.StatusBadRequest)
		return
	}

	s.engine.SetDLQ(topic, req.DLQTopic)
	w.WriteHeader(http.StatusOK)
	w.Write([]byte("DLQ " + req.DLQTopic + " registered for topic " + topic))
}

func (s *Server) handlePublish(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		WriteJSONError(w, r, "Method not allowed", "ERR_METHOD_NOT_ALLOWED", http.StatusMethodNotAllowed)
		return
	}

	var req struct {
		Topic     string `json:"topic"`
		Payload   string `json:"payload"`
		MessageID string `json:"message_id,omitempty"`
	}

	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		WriteJSONError(w, r, "Bad request", "ERR_BAD_REQUEST_BODY", http.StatusBadRequest)
		return
	}

	// Propagate traceparent header if received
	ctx := r.Context()
	if tp := r.Header.Get("traceparent"); tp != "" {
		ctx = context.WithValue(ctx, "traceparent", tp)
	}
	if req.MessageID != "" {
		ctx = context.WithValue(ctx, "message-id", req.MessageID)
	} else if msgID := r.Header.Get("Message-Id"); msgID != "" {
		ctx = context.WithValue(ctx, "message-id", msgID)
	}

	res, err := s.engine.Publish(ctx, req.Topic, req.Payload)
	if err != nil {
		if err.Error() == "rate limit exceeded" {
			WriteJSONError(w, r, "Rate limit exceeded", "ERR_RATE_LIMIT_EXCEEDED", http.StatusTooManyRequests)
			return
		}
		WriteJSONError(w, r, "Transform error: "+err.Error(), "ERR_WASM_TRANSFORM_FAILED", http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]string{"status": "ok", "delivered_payload": res})
}

func (s *Server) handleStats(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	
	walEntries, _ := s.engine.GetWALEntries()
	if walEntries == nil {
		walEntries = []storage.LogEntry{}
	}
	delayedMsgs := s.engine.GetDelayedMessages()
	if delayedMsgs == nil {
		delayedMsgs = []broker.DelayedMessage{}
	}

	json.NewEncoder(w).Encode(map[string]interface{}{
		"status": "healthy",
		"metrics": map[string]interface{}{
			"messages_published_total": s.engine.Metrics.MessagesPublished,
			"wasm_executions_total":    s.engine.Metrics.WasmExecutions,
			"wasm_execution_errors":    s.engine.Metrics.WasmExecutionErrors,
			"wasm_duration_ns":         s.engine.Metrics.WasmDurationNs,
		},
		"wal_entries":      walEntries,
		"delayed_messages": delayedMsgs,
	})
}

func (s *Server) handleReplay(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Topic  string `json:"topic"`
		Offset int64  `json:"offset"`
		Group  string `json:"group,omitempty"`
	}

	switch r.Method {
	case http.MethodPost:
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			WriteJSONError(w, r, "Bad request: invalid JSON body", "ERR_BAD_REQUEST_BODY", http.StatusBadRequest)
			return
		}
	case http.MethodGet:
		req.Topic = r.URL.Query().Get("topic")
		req.Group = r.URL.Query().Get("group")
		if offStr := r.URL.Query().Get("offset"); offStr != "" {
			if parsed, err := strconv.ParseInt(offStr, 10, 64); err == nil {
				req.Offset = parsed
			}
		}
	default:
		WriteJSONError(w, r, "Method not allowed", "ERR_METHOD_NOT_ALLOWED", http.StatusMethodNotAllowed)
		return
	}

	if req.Topic == "" {
		WriteJSONError(w, r, "Missing topic parameter", "ERR_MISSING_TOPIC_PARAMETER", http.StatusBadRequest)
		return
	}

	records, err := s.engine.ReplayMessages(r.Context(), req.Topic, req.Offset, req.Group)
	if err != nil {
		WriteJSONError(w, r, "Replay failed: "+err.Error(), "ERR_REPLAY_FAILED", http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{
		"status":  "replay_completed",
		"topic":   req.Topic,
		"records": records,
	})
}

func (s *Server) handleOffsets(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodGet:
		group := r.URL.Query().Get("group")
		topic := r.URL.Query().Get("topic")
		if group == "" || topic == "" {
			WriteJSONError(w, r, "Missing group or topic query parameters", "ERR_MISSING_PARAMETERS", http.StatusBadRequest)
			return
		}
		offset := s.engine.GetOffset(group, topic)
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]interface{}{
			"group":  group,
			"topic":  topic,
			"offset": offset,
		})
	case http.MethodPost:
		var req struct {
			Group  string `json:"group"`
			Topic  string `json:"topic"`
			Offset int64  `json:"offset"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			WriteJSONError(w, r, "Bad request: invalid JSON body", "ERR_BAD_REQUEST_BODY", http.StatusBadRequest)
			return
		}
		if req.Group == "" || req.Topic == "" {
			WriteJSONError(w, r, "Missing group or topic in JSON body", "ERR_MISSING_PARAMETERS", http.StatusBadRequest)
			return
		}
		s.engine.CommitOffset(req.Group, req.Topic, req.Offset)
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]interface{}{
			"status":  "success",
			"message": "Offset committed successfully",
		})
	default:
		WriteJSONError(w, r, "Method not allowed", "ERR_METHOD_NOT_ALLOWED", http.StatusMethodNotAllowed)
	}
}

func validateJWT(tokenStr string, secret []byte) (string, bool) {
	parts := strings.Split(tokenStr, ".")
	if len(parts) != 3 {
		return "", false
	}

	headerPart, payloadPart, signaturePart := parts[0], parts[1], parts[2]
	
	// Validate signature
	mac := hmac.New(sha256.New, secret)
	mac.Write([]byte(headerPart + "." + payloadPart))
	expectedMac := mac.Sum(nil)
	
	// Base64Url decode signaturePart
	sigBytes, err := base64UrlDecode(signaturePart)
	if err != nil || !hmac.Equal(sigBytes, expectedMac) {
		return "", false
	}

	// Base64Url decode payloadPart and extract username, exp
	payloadBytes, err := base64UrlDecode(payloadPart)
	if err != nil {
		return "", false
	}

	var claims struct {
		Username string `json:"username"`
		Exp      int64  `json:"exp"`
	}
	if err := json.Unmarshal(payloadBytes, &claims); err != nil {
		return "", false
	}

	// Check expiration
	if claims.Exp > 0 && time.Now().Unix() > claims.Exp {
		return "", false
	}

	return claims.Username, true
}

func base64UrlDecode(s string) ([]byte, error) {
	if l := len(s) % 4; l > 0 {
		s += strings.Repeat("=", 4-l)
	}
	s = strings.ReplaceAll(s, "-", "+")
	s = strings.ReplaceAll(s, "_", "/")
	return base64.URLEncoding.DecodeString(s)
}

var upgrader = websocket.Upgrader{
	CheckOrigin: func(r *http.Request) bool {
		return true
	},
}

func (s *Server) handleStatsWS(w http.ResponseWriter, r *http.Request) {
	conn, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		log.Printf("Failed to upgrade websocket: %v", err)
		return
	}
	defer conn.Close()

	ticker := time.NewTicker(100 * time.Millisecond) // tick faster in testing/control updates
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			walEntries, _ := s.engine.GetWALEntries()
			if walEntries == nil {
				walEntries = []storage.LogEntry{}
			}
			delayedMsgs := s.engine.GetDelayedMessages()
			if delayedMsgs == nil {
				delayedMsgs = []broker.DelayedMessage{}
			}

			stats := map[string]interface{}{
				"status": "healthy",
				"metrics": map[string]interface{}{
					"messages_published_total": s.engine.Metrics.MessagesPublished,
					"wasm_executions_total":    s.engine.Metrics.WasmExecutions,
					"wasm_execution_errors":    s.engine.Metrics.WasmExecutionErrors,
					"wasm_duration_ns":         s.engine.Metrics.WasmDurationNs,
				},
				"wal_entries":      walEntries,
				"delayed_messages": delayedMsgs,
			}

			if err := conn.WriteJSON(stats); err != nil {
				return
			}
		case <-r.Context().Done():
			return
		}
	}
}
