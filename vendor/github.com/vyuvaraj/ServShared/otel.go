package ServShared

import (
	"bytes"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"strings"
	"sync"
	"time"
)

type Span struct {
	TraceID    string                 `json:"traceId"`
	SpanID     string                 `json:"spanId"`
	ParentID   string                 `json:"parentSpanId,omitempty"`
	Name       string                 `json:"name"`
	Kind       int                    `json:"kind"` // 1=internal, 2=server, 3=client
	StartTime  int64                  `json:"startTimeUnixNano"`
	EndTime    int64                  `json:"endTimeUnixNano"`
	Attributes map[string]interface{} `json:"attributes,omitempty"`
	Status     int                    `json:"status"` // 1=ok, 2=error
}

type TelemetryProvider struct {
	mu                sync.Mutex
	enabled           bool
	endpoint          string
	serviceName       string
	spanBuffer        []Span
	spanFlushInterval time.Duration
	maxBatchSize      int
	closeChan         chan struct{}
	wg                sync.WaitGroup
}

var (
	GlobalTelemetry = &TelemetryProvider{
		spanFlushInterval: 5 * time.Second,
		maxBatchSize:      100,
		closeChan:         make(chan struct{}),
	}
)

// InitTrace initializes the global tracing provider.
func InitTrace(serviceName string) {
	GlobalTelemetry.Init(serviceName)
}

func (tp *TelemetryProvider) Init(serviceName string) {
	tp.mu.Lock()
	defer tp.mu.Unlock()

	endpoint := getOtelEndpoint()
	if endpoint == "" {
		return
	}

	tp.endpoint = strings.TrimSuffix(endpoint, "/")
	tp.enabled = true
	tp.serviceName = os.Getenv("OTEL_SERVICE_NAME")
	if tp.serviceName == "" {
		tp.serviceName = serviceName
	}

	tp.wg.Add(1)
	go tp.flushLoop()
}

// Shutdown flushes and closes tracing.
func Shutdown() {
	GlobalTelemetry.Close()
}

func (tp *TelemetryProvider) Close() {
	tp.mu.Lock()
	if !tp.enabled {
		tp.mu.Unlock()
		return
	}
	tp.enabled = false
	close(tp.closeChan)
	tp.mu.Unlock()

	// Wait for loop to finish
	tp.wg.Wait()

	// Flush remaining spans
	tp.mu.Lock()
	if len(tp.spanBuffer) > 0 {
		tp.exportSpans(tp.spanBuffer)
		tp.spanBuffer = nil
	}
	tp.mu.Unlock()
}

func getOtelEndpoint() string {
	if ep := os.Getenv("OTEL_ENDPOINT"); ep != "" {
		return ep
	}
	if ep := os.Getenv("SERV_OTLP_ENDPOINT"); ep != "" {
		return ep
	}
	if raw := os.Getenv("SERVVERSE_DISCOVERY"); raw != "" {
		var manifest struct {
			OTLPEndpoint string `json:"otlp_endpoint"`
		}
		if json.Unmarshal([]byte(raw), &manifest) == nil {
			if manifest.OTLPEndpoint != "" {
				return manifest.OTLPEndpoint
			}
		} else {
			if data, err := os.ReadFile(raw); err == nil {
				if json.Unmarshal(data, &manifest) == nil && manifest.OTLPEndpoint != "" {
					return manifest.OTLPEndpoint
				}
			}
		}
	}
	return ""
}

func GenerateTraceID() string {
	b := make([]byte, 16)
	_, _ = rand.Read(b)
	return hex.EncodeToString(b)
}

func GenerateSpanID() string {
	b := make([]byte, 8)
	_, _ = rand.Read(b)
	return hex.EncodeToString(b)
}

func StartSpan(name string, parentTrace string) *Span {
	return GlobalTelemetry.StartSpan(name, parentTrace)
}

func (tp *TelemetryProvider) StartSpan(name string, parentTrace string) *Span {
	tp.mu.Lock()
	enabled := tp.enabled
	tp.mu.Unlock()

	if !enabled {
		return nil
	}

	traceID := ""
	parentSpan := ""
	if parentTrace != "" {
		parts := strings.Split(parentTrace, "-")
		if len(parts) >= 4 {
			traceID = parts[1]
			parentSpan = parts[2]
		}
	}

	if traceID == "" {
		traceID = GenerateTraceID()
	}

	return &Span{
		TraceID:   traceID,
		SpanID:    GenerateSpanID(),
		ParentID:  parentSpan,
		Name:      name,
		Kind:      1, // Internal default
		StartTime: time.Now().UnixNano(),
	}
}

func EndSpan(span *Span, err error, attributes map[string]interface{}) {
	GlobalTelemetry.EndSpan(span, err, attributes)
}

func (tp *TelemetryProvider) EndSpan(span *Span, err error, attributes map[string]interface{}) {
	tp.mu.Lock()
	enabled := tp.enabled
	tp.mu.Unlock()

	if !enabled || span == nil {
		return
	}

	span.EndTime = time.Now().UnixNano()
	span.Status = 1 // OK
	if err != nil {
		span.Status = 2 // Error
		if attributes == nil {
			attributes = make(map[string]interface{})
		}
		attributes["error.message"] = err.Error()
	}
	if span.Attributes == nil {
		span.Attributes = make(map[string]interface{})
	}
	for k, v := range attributes {
		span.Attributes[k] = v
	}

	tp.mu.Lock()
	tp.spanBuffer = append(tp.spanBuffer, *span)
	if len(tp.spanBuffer) >= tp.maxBatchSize {
		batch := tp.spanBuffer
		tp.spanBuffer = nil
		tp.mu.Unlock()
		go tp.exportSpans(batch)
	} else {
		tp.mu.Unlock()
	}
}

func (tp *TelemetryProvider) flushLoop() {
	defer tp.wg.Done()
	ticker := time.NewTicker(tp.spanFlushInterval)
	defer ticker.Stop()

	for {
		select {
		case <-tp.closeChan:
			return
		case <-ticker.C:
			tp.mu.Lock()
			if len(tp.spanBuffer) == 0 {
				tp.mu.Unlock()
				continue
			}
			batch := tp.spanBuffer
			tp.spanBuffer = nil
			tp.mu.Unlock()

			tp.exportSpans(batch)
		}
	}
}

func (tp *TelemetryProvider) exportSpans(spans []Span) {
	tp.mu.Lock()
	endpoint := tp.endpoint
	serviceName := tp.serviceName
	tp.mu.Unlock()

	payload := map[string]interface{}{
		"resourceSpans": []map[string]interface{}{
			{
				"resource": map[string]interface{}{
					"attributes": []map[string]interface{}{
						{"key": "service.name", "value": map[string]interface{}{"stringValue": serviceName}},
					},
				},
				"scopeSpans": []map[string]interface{}{
					{
						"scope": map[string]interface{}{"name": "servverse-shared"},
						"spans": buildSpanPayload(spans),
					},
				},
			},
		},
	}

	body, err := json.Marshal(payload)
	if err != nil {
		return
	}

	url := endpoint + "/v1/traces"
	client := &http.Client{Timeout: 3 * time.Second}
	resp, err := client.Post(url, "application/json", bytes.NewReader(body))
	if err != nil {
		return
	}
	_ = resp.Body.Close()
}

func buildSpanPayload(spans []Span) []map[string]interface{} {
	result := make([]map[string]interface{}, 0, len(spans))
	for _, s := range spans {
		span := map[string]interface{}{
			"traceId":            s.TraceID,
			"spanId":             s.SpanID,
			"name":              s.Name,
			"kind":              s.Kind,
			"startTimeUnixNano": fmt.Sprintf("%d", s.StartTime),
			"endTimeUnixNano":   fmt.Sprintf("%d", s.EndTime),
			"status":            map[string]interface{}{"code": s.Status},
		}
		if s.ParentID != "" {
			span["parentSpanId"] = s.ParentID
		}
		if len(s.Attributes) > 0 {
			attrs := make([]map[string]interface{}, 0)
			for k, v := range s.Attributes {
				attrs = append(attrs, map[string]interface{}{
					"key":   k,
					"value": map[string]interface{}{"stringValue": fmt.Sprint(v)},
				})
			}
			span["attributes"] = attrs
		}
		result = append(result, span)
	}
	return result
}
