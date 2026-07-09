package web

import (
	"encoding/json"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"
)

type Event struct {
	Sequence  int64     `json:"seq"`
	Type      string    `json:"type"`
	Payload   string    `json:"payload"`
	Timestamp time.Time `json:"timestamp"`
}

var (
	eventsMu  sync.RWMutex
	eventLogs = make(map[string][]Event)
)

func init() {
	os.MkdirAll("events", 0755)
	files, err := os.ReadDir("events")
	if err == nil {
		for _, f := range files {
			if !f.IsDir() && strings.HasSuffix(f.Name(), ".json") {
				streamName := strings.TrimSuffix(f.Name(), ".json")
				filePath := filepath.Join("events", f.Name())
				file, err := os.Open(filePath)
				if err == nil {
					dec := json.NewDecoder(file)
					var list []Event
					for {
						var ev Event
						if err := dec.Decode(&ev); err != nil {
							break
						}
						list = append(list, ev)
					}
					file.Close()
					eventLogs[streamName] = list
				}
			}
		}
	}
}

func (s *Server) handleEvents(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")

	parts := strings.Split(r.URL.Path, "/")
	if len(parts) < 5 {
		http.Error(w, `{"error":"invalid path"}`, http.StatusBadRequest)
		return
	}
	stream := parts[4]
	if stream == "" {
		http.Error(w, `{"error":"stream name required"}`, http.StatusBadRequest)
		return
	}

	if len(parts) >= 6 && parts[5] == "latest" {
		eventsMu.RLock()
		list := eventLogs[stream]
		eventsMu.RUnlock()

		if len(list) == 0 {
			json.NewEncoder(w).Encode(map[string]interface{}{"seq": 0})
			return
		}
		json.NewEncoder(w).Encode(map[string]interface{}{
			"seq": list[len(list)-1].Sequence,
		})
		return
	}

	switch r.Method {
	case http.MethodPost:
		var input struct {
			Type    string `json:"type"`
			Payload string `json:"payload"`
		}
		if err := json.NewDecoder(r.Body).Decode(&input); err != nil {
			http.Error(w, `{"error":"invalid payload"}`, http.StatusBadRequest)
			return
		}

		eventsMu.Lock()
		defer eventsMu.Unlock()

		list := eventLogs[stream]
		newSeq := int64(len(list)) + 1
		ev := Event{
			Sequence:  newSeq,
			Type:      input.Type,
			Payload:   input.Payload,
			Timestamp: time.Now(),
		}
		list = append(list, ev)
		eventLogs[stream] = list

		f, err := os.OpenFile(filepath.Join("events", stream+".json"), os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
		if err == nil {
			json.NewEncoder(f).Encode(ev)
			f.Close()
		}

		w.WriteHeader(http.StatusCreated)
		json.NewEncoder(w).Encode(ev)

	case http.MethodGet:
		eventsMu.RLock()
		list := eventLogs[stream]
		eventsMu.RUnlock()

		fromSeqStr := r.URL.Query().Get("from")
		var fromSeq int64 = 0
		if fromSeqStr != "" {
			if parsed, err := strconv.ParseInt(fromSeqStr, 10, 64); err == nil {
				fromSeq = parsed
			}
		}

		var filtered []Event
		for _, ev := range list {
			if ev.Sequence >= fromSeq {
				filtered = append(filtered, ev)
			}
		}

		json.NewEncoder(w).Encode(filtered)

	default:
		http.Error(w, `{"error":"method not allowed"}`, http.StatusMethodNotAllowed)
	}
}
