package broker

import (
	"context"
	"fmt"
	"hash/fnv"
	"log"
	"sync"
	"sync/atomic"
	"time"

	"servqueue/pkg/otel"
	"servqueue/pkg/storage"

	"github.com/tetratelabs/wazero"
)

type Subscriber struct {
	ID chan string
}

type BrokerMetrics struct {
	MessagesPublished   uint64
	WasmExecutions      uint64
	WasmExecutionErrors uint64
	WasmDurationNs      uint64
}

type BrokerEngine struct {
	mu          sync.RWMutex
	topics      map[string][]chan string
	partitions  map[string]map[int][]chan string // Topic -> PartitionID -> SubChannels
	transforms  map[string]wazero.CompiledModule
	dlqTopics   map[string]string                // Topic -> DLQ topic name
	wasmManager *WasmManager
	Metrics     BrokerMetrics
	wal         *storage.WAL
	raftNode    interface{}
	offloader   *storage.Offloader
	dedup       *Deduplicator
}

func NewBrokerEngine() *BrokerEngine {
	mgr, err := GetWasmManager(context.Background())
	if err != nil {
		panic(fmt.Sprintf("Failed to initialize WASM Manager: %v", err))
	}

	wal, walErr := storage.OpenWAL("queue.wal")
	if walErr != nil {
		wal = nil
	}

	engine := &BrokerEngine{
		topics:      make(map[string][]chan string),
		partitions:  make(map[string]map[int][]chan string),
		transforms:  make(map[string]wazero.CompiledModule),
		dlqTopics:   make(map[string]string),
		wasmManager: mgr,
		wal:         wal,
		dedup:       NewDeduplicator(5 * time.Minute),
	}

	if wal != nil {
		// Set rotation trigger to upload closed segment to cold storage
		wal.OnRotate = func(closedPath string) {
			if engine.offloader != nil {
				_ = engine.offloader.OffloadSegment(closedPath)
			}
		}

		if entries, recoverErr := wal.Recover(); recoverErr == nil {
			for _, entry := range entries {
				_, _ = engine.publishLocal(context.Background(), entry.Topic, entry.Payload)
			}
		}
	}

	return engine
}

func (e *BrokerEngine) ConfigureOffloader(endpoint, bucket, token string) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.offloader = storage.NewOffloader(endpoint, bucket, token)
}

func (e *BrokerEngine) SetRaftNode(node interface{}) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.raftNode = node
}

// SetDLQ registers a dead letter queue topic for a source topic.
// When a WASM transform fails for the source topic, the original
// payload is routed to the DLQ topic instead of being silently dropped.
func (e *BrokerEngine) SetDLQ(topic string, dlqTopic string) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.dlqTopics[topic] = dlqTopic
}

// GetDLQ returns the DLQ topic registered for a given source topic, if any.
func (e *BrokerEngine) GetDLQ(topic string) (string, bool) {
	e.mu.RLock()
	defer e.mu.RUnlock()
	dlq, ok := e.dlqTopics[topic]
	return dlq, ok
}

// routeToDLQ publishes payload to the DLQ topic if one is registered,
// incrementing the DLQ delivery counter.
func (e *BrokerEngine) routeToDLQ(ctx context.Context, sourceTopic string, payload string, reason string) {
	dlqTopic, ok := e.GetDLQ(sourceTopic)
	if !ok {
		// No DLQ configured — message is dropped (legacy behaviour)
		return
	}

	envelope := fmt.Sprintf(`{"dlq":true,"source_topic":%q,"reason":%q,"payload":%q}`,
		sourceTopic, reason, payload)

	// Deliver directly to DLQ subscribers without running transforms
	e.mu.RLock()
	subs := e.topics[dlqTopic]
	e.mu.RUnlock()
	for _, sub := range subs {
		select {
		case sub <- envelope:
		case <-ctx.Done():
			return
		default:
		}
	}
	atomic.AddUint64(&e.Metrics.MessagesPublished, 1)
	if e.wal != nil {
		_ = e.wal.Append(dlqTopic, envelope)
	}
}

// Subscribe adds a subscriber channel to a topic
func (e *BrokerEngine) Subscribe(topic string) chan string {
	e.mu.Lock()
	defer e.mu.Unlock()

	ch := make(chan string, 100)
	e.topics[topic] = append(e.topics[topic], ch)
	return ch
}

// SubscribePartition registers a subscriber to a specific partition index of a topic
func (e *BrokerEngine) SubscribePartition(topic string, partition int) chan string {
	e.mu.Lock()
	defer e.mu.Unlock()

	ch := make(chan string, 100)
	if e.partitions[topic] == nil {
		e.partitions[topic] = make(map[int][]chan string)
	}
	e.partitions[topic][partition] = append(e.partitions[topic][partition], ch)
	return ch
}

// Unsubscribe removes a subscriber channel from a topic
func (e *BrokerEngine) Unsubscribe(topic string, ch chan string) {
	e.mu.Lock()
	defer e.mu.Unlock()

	subs, exists := e.topics[topic]
	if exists {
		for i, sub := range subs {
			if sub == ch {
				e.topics[topic] = append(subs[:i], subs[i+1:]...)
				close(ch)
				return
			}
		}
	}

	partsMap, hasPart := e.partitions[topic]
	if hasPart {
		for partId, chs := range partsMap {
			for i, sub := range chs {
				if sub == ch {
					e.partitions[topic][partId] = append(chs[:i], chs[i+1:]...)
					close(ch)
					return
				}
			}
		}
	}
}

// RegisterTransform compiles and sets the WASM transform module for a topic
func (e *BrokerEngine) RegisterTransform(ctx context.Context, topic string, wasmBytes []byte) error {
	e.mu.Lock()
	defer e.mu.Unlock()

	if len(wasmBytes) == 0 {
		if compiled, exists := e.transforms[topic]; exists {
			_ = compiled.Close(ctx)
			delete(e.transforms, topic)
		}
		return nil
	}

	compiled, err := e.wasmManager.Compile(ctx, wasmBytes)
	if err != nil {
		return fmt.Errorf("failed to compile WASM module: %w", err)
	}

	if old, exists := e.transforms[topic]; exists {
		_ = old.Close(ctx)
	}

	e.transforms[topic] = compiled
	return nil
}

// PublishPartition hashes the key to dispatch messages to a specific partitioned queue
func (e *BrokerEngine) PublishPartition(ctx context.Context, topic string, key string, payload string) (string, error) {
	// Hash the key using FNV-1a
	hasher := fnv.New32a()
	hasher.Write([]byte(key))
	partitionId := int(hasher.Sum32() % 3) // Assume 3 default partitions

	atomic.AddUint64(&e.Metrics.MessagesPublished, 1)
	if e.wal != nil {
		_ = e.wal.Append(topic, payload)
	}

	var parentTrace string
	if traceparentVal, ok := ctx.Value("traceparent").(string); ok {
		parentTrace = traceparentVal
	}
	span := otel.StartSpan(fmt.Sprintf("PublishPartition %s-%d", topic, partitionId), parentTrace)

	e.mu.RLock()
	compiledModule, hasTransform := e.transforms[topic]
	e.mu.RUnlock()

	var err error
	processed := payload
	if hasTransform && compiledModule != nil {
		atomic.AddUint64(&e.Metrics.WasmExecutions, 1)
		var wasmParentTrace string
		if span != nil {
			wasmParentTrace = fmt.Sprintf("00-%s-%s-01", span.TraceID, span.SpanID)
		}
		wasmSpan := otel.StartSpan(fmt.Sprintf("WASM Transform %s", topic), wasmParentTrace)

		start := time.Now()
		processed, err = e.wasmManager.RunTransform(ctx, compiledModule, payload, wasmParentTrace)
		duration := time.Since(start)

		atomic.AddUint64(&e.Metrics.WasmDurationNs, uint64(duration.Nanoseconds()))

		otel.EndSpan(wasmSpan, err, map[string]interface{}{
			"wasm.duration_ms": duration.Milliseconds(),
			"wasm.topic":       topic,
		})

		if err != nil {
			atomic.AddUint64(&e.Metrics.WasmExecutionErrors, 1)
			otel.EndSpan(span, err, map[string]interface{}{})
			e.routeToDLQ(ctx, topic, payload, err.Error())
			return payload, err
		}
	}

	// Dispatch to the targeted partition index subs
	e.mu.RLock()
	partsMap, exists := e.partitions[topic]
	var subs []chan string
	if exists {
		subs = partsMap[partitionId]
	}
	e.mu.RUnlock()

	for _, sub := range subs {
		select {
		case sub <- processed:
		default:
		}
	}

	otel.EndSpan(span, nil, map[string]interface{}{})
	return processed, nil
}

// Publish writes a message to a topic, running any registered WASM transform first
func (e *BrokerEngine) Publish(ctx context.Context, topic string, payload string) (string, error) {
	if msgID, ok := ctx.Value("message-id").(string); ok && msgID != "" {
		if !e.dedup.Add(msgID) {
			log.Printf("Broker: duplicate message detected for message-id: %s. Dropping.", msgID)
			return payload, fmt.Errorf("duplicate message detected: %s", msgID)
		}
	}

	atomic.AddUint64(&e.Metrics.MessagesPublished, 1)

	if e.wal != nil {
		_ = e.wal.Append(topic, payload)
	}

	var parentTrace string
	if traceparentVal, ok := ctx.Value("traceparent").(string); ok {
		parentTrace = traceparentVal
	}

	span := otel.StartSpan(fmt.Sprintf("Publish %s", topic), parentTrace)

	e.mu.RLock()
	compiledModule, hasTransform := e.transforms[topic]
	e.mu.RUnlock()

	var err error
	processed := payload
	if hasTransform && compiledModule != nil {
		atomic.AddUint64(&e.Metrics.WasmExecutions, 1)

		var wasmParentTrace string
		if span != nil {
			wasmParentTrace = fmt.Sprintf("00-%s-%s-01", span.TraceID, span.SpanID)
		}
		wasmSpan := otel.StartSpan(fmt.Sprintf("WASM Transform %s", topic), wasmParentTrace)

		start := time.Now()
		processed, err = e.wasmManager.RunTransform(ctx, compiledModule, payload, wasmParentTrace)
		duration := time.Since(start)

		atomic.AddUint64(&e.Metrics.WasmDurationNs, uint64(duration.Nanoseconds()))

		otel.EndSpan(wasmSpan, err, map[string]interface{}{
			"wasm.duration_ms": duration.Milliseconds(),
			"wasm.topic":       topic,
		})

		if err != nil {
			atomic.AddUint64(&e.Metrics.WasmExecutionErrors, 1)
			otel.EndSpan(span, err, map[string]interface{}{
				"messaging.destination": topic,
			})
			e.routeToDLQ(ctx, topic, payload, err.Error())
			return payload, err
		}
	}

	e.mu.RLock()
	subs, exists := e.topics[topic]
	e.mu.RUnlock()

	if exists {
		for _, sub := range subs {
			select {
			case sub <- processed:
			default:
			}
		}
	}

	otel.EndSpan(span, nil, map[string]interface{}{
		"messaging.system":      "servqueue",
		"messaging.destination": topic,
		"messaging.payload_len": len(processed),
	})

	return processed, nil
}

func (e *BrokerEngine) publishLocal(ctx context.Context, topic string, payload string) (string, error) {
	var parentTrace string
	if traceparentVal, ok := ctx.Value("traceparent").(string); ok {
		parentTrace = traceparentVal
	}

	span := otel.StartSpan(fmt.Sprintf("PublishLocal %s", topic), parentTrace)

	e.mu.RLock()
	compiledModule, hasTransform := e.transforms[topic]
	e.mu.RUnlock()

	var err error
	processed := payload
	if hasTransform && compiledModule != nil {
		var wasmParentTrace string
		if span != nil {
			wasmParentTrace = fmt.Sprintf("00-%s-%s-01", span.TraceID, span.SpanID)
		}
		wasmSpan := otel.StartSpan(fmt.Sprintf("WASM Transform %s", topic), wasmParentTrace)

		start := time.Now()
		processed, err = e.wasmManager.RunTransform(ctx, compiledModule, payload, wasmParentTrace)
		duration := time.Since(start)

		otel.EndSpan(wasmSpan, err, map[string]interface{}{
			"wasm.duration_ms": duration.Milliseconds(),
			"wasm.topic":       topic,
		})

		if err != nil {
			otel.EndSpan(span, err, map[string]interface{}{})
			e.routeToDLQ(ctx, topic, payload, err.Error())
			return payload, err
		}
	}

	e.mu.RLock()
	subs, exists := e.topics[topic]
	e.mu.RUnlock()

	if exists {
		for _, sub := range subs {
			select {
			case sub <- processed:
			default:
			}
		}
	}

	otel.EndSpan(span, nil, map[string]interface{}{})
	return processed, nil
}
