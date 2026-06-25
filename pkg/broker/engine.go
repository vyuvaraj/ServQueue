package broker

import (
	"context"
	"fmt"
	"hash/fnv"
	"log"
	"os"
	"strconv"
	"strings"
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

type DelayedMessage struct {
	ID         string    `json:"id"`
	Topic      string    `json:"topic"`
	Payload    string    `json:"payload"`
	TargetTime time.Time `json:"target_time"`
}

type BrokerEngine struct {
	mu             sync.RWMutex
	topics         map[string][]chan string
	partitions     map[string]map[int][]chan string // Topic -> PartitionID -> SubChannels
	groupSubs      map[string]map[string][]chan string // Topic -> GroupName -> Subscriber channels
	groupIndices   map[string]map[string]uint64        // Topic -> GroupName -> Round-robin counter
	transforms     map[string]wazero.CompiledModule
	dlqTopics      map[string]string                // Topic -> DLQ topic name
	wasmManager    *WasmManager
	Metrics        BrokerMetrics
	wal            *storage.WAL
	raftNode       interface{}
	offloader      *storage.Offloader
	dedup          *Deduplicator
	delayedMu      sync.Mutex
	delayedMsgs    map[string]DelayedMessage
	delayedCounter uint64
	groupOffsets      map[string]map[string]int64 // groupName -> topic -> offset
	timeWheel         *TimeWheel
	publishLimiter    *TokenBucket
	topicQueues       map[string]*PriorityQueue
	topicCancel       map[string]context.CancelFunc
	producerMu        sync.Mutex
	producerSequences map[string]int64
	schemasMu         sync.RWMutex
	schemas           map[string]map[string]string
	compactedTopicsMu sync.RWMutex
	compactedTopics   map[string]bool
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
		topics:       make(map[string][]chan string),
		partitions:   make(map[string]map[int][]chan string),
		groupSubs:    make(map[string]map[string][]chan string),
		groupIndices: make(map[string]map[string]uint64),
		transforms:   make(map[string]wazero.CompiledModule),
		dlqTopics:    make(map[string]string),
		wasmManager:  mgr,
		wal:          wal,
		dedup:        NewDeduplicator(5 * time.Minute),
		delayedMsgs:  make(map[string]DelayedMessage),
		groupOffsets:      make(map[string]map[string]int64),
		timeWheel:         NewTimeWheel(10*time.Millisecond, 36000),
		topicQueues:       make(map[string]*PriorityQueue),
		topicCancel:       make(map[string]context.CancelFunc),
		producerSequences: make(map[string]int64),
		schemas:           make(map[string]map[string]string),
		compactedTopics:   make(map[string]bool),
	}
	engine.timeWheel.Start()

	rate := 100.0
	capacity := 100.0
	if rateStr := os.Getenv("SERVQUEUE_PUBLISH_RATE"); rateStr != "" {
		if r, err := strconv.ParseFloat(rateStr, 64); err == nil && r > 0 {
			rate = r
		}
	}
	if capStr := os.Getenv("SERVQUEUE_PUBLISH_CAPACITY"); capStr != "" {
		if c, err := strconv.ParseFloat(capStr, 64); err == nil && c > 0 {
			capacity = c
		}
	}
	engine.publishLimiter = NewTokenBucket(rate, capacity)

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

	if s3Endpoint := os.Getenv("SERVQUEUE_S3_ENDPOINT"); s3Endpoint != "" {
		s3Bucket := os.Getenv("SERVQUEUE_S3_BUCKET")
		s3Token := os.Getenv("SERVQUEUE_S3_TOKEN")
		engine.ConfigureOffloader(s3Endpoint, s3Bucket, s3Token)
	}

	return engine
}

// Stop stops the background workers (like the TimeWheel ticker).
func (e *BrokerEngine) Stop() {
	if e.timeWheel != nil {
		e.timeWheel.Stop()
	}
	e.mu.Lock()
	for _, cancel := range e.topicCancel {
		cancel()
	}
	e.mu.Unlock()
}

func (e *BrokerEngine) getOrCreateQueue(topic string) *PriorityQueue {
	e.mu.Lock()
	defer e.mu.Unlock()

	pq, ok := e.topicQueues[topic]
	if !ok {
		pq = NewPriorityQueue()
		e.topicQueues[topic] = pq

		ctx, cancel := context.WithCancel(context.Background())
		e.topicCancel[topic] = cancel

		go e.dispatchLoop(ctx, topic, pq)
	}
	return pq
}

func (e *BrokerEngine) dispatchLoop(ctx context.Context, topic string, pq *PriorityQueue) {
	for {
		select {
		case <-ctx.Done():
			return
		default:
			e.mu.RLock()
			hasSubs := false
			for pattern, patternSubs := range e.topics {
				if len(patternSubs) > 0 && matchTopic(pattern, topic) {
					hasSubs = true
					break
				}
			}
			if !hasSubs {
				for pattern, partsMap := range e.partitions {
					if matchTopic(pattern, topic) {
						for _, pSubs := range partsMap {
							if len(pSubs) > 0 {
								hasSubs = true
								break
							}
						}
					}
					if hasSubs {
						break
					}
				}
			}
			if !hasSubs {
				for pattern, gSubsMap := range e.groupSubs {
					if matchTopic(pattern, topic) {
						for _, gSubs := range gSubsMap {
							if len(gSubs) > 0 {
								hasSubs = true
								break
							}
						}
					}
					if hasSubs {
						break
					}
				}
			}
			e.mu.RUnlock()

			if !hasSubs {
				select {
				case <-ctx.Done():
					return
				case <-time.After(10 * time.Millisecond):
					continue
				}
			}

			msg, ok := pq.PopNonBlocking()
			if !ok {
				select {
				case <-ctx.Done():
					return
				case <-time.After(10 * time.Millisecond):
					continue
				}
			}

			// Expiry / TTL Check
			if !msg.Expiry.IsZero() && time.Now().After(msg.Expiry) {
				e.routeToDLQ(ctx, topic, msg.Payload, "message TTL expired")
				continue
			}

			e.mu.RLock()
			var subs []chan string
			for pattern, patternSubs := range e.topics {
				if matchTopic(pattern, topic) {
					subs = append(subs, patternSubs...)
				}
			}
			var partitionSubs []chan string
			for pattern, partsMap := range e.partitions {
				if matchTopic(pattern, topic) {
					if chs, ok := partsMap[msg.PartitionId]; ok {
						partitionSubs = append(partitionSubs, chs...)
					}
				}
			}
			groupsMap := make(map[string][]chan string)
			for pattern, gSubsMap := range e.groupSubs {
				if matchTopic(pattern, topic) {
					for gName, gSubs := range gSubsMap {
						groupsMap[gName] = append(groupsMap[gName], gSubs...)
					}
				}
			}
			e.mu.RUnlock()

			for _, sub := range subs {
				select {
				case sub <- msg.Payload:
				default:
				}
			}

			for _, sub := range partitionSubs {
				select {
				case sub <- msg.Payload:
				default:
				}
			}

			for gName, gSubs := range groupsMap {
				if len(gSubs) == 0 {
					continue
				}
				e.mu.Lock()
				if e.groupIndices[topic] == nil {
					e.groupIndices[topic] = make(map[string]uint64)
				}
				idx := e.groupIndices[topic][gName]
				e.groupIndices[topic][gName] = idx + 1
				e.mu.Unlock()

				targetChan := gSubs[idx%uint64(len(gSubs))]
				select {
				case targetChan <- msg.Payload:
				default:
				}
			}
		}
	}
}

func (e *BrokerEngine) ConfigureOffloader(endpoint, bucket, token string) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.offloader = storage.NewOffloader(endpoint, bucket, token)
}

func (e *BrokerEngine) GetOffloader() *storage.Offloader {
	e.mu.RLock()
	defer e.mu.RUnlock()
	return e.offloader
}

// TopicInfo describes the state of a single topic for admin inspection.
type TopicInfo struct {
	Name         string `json:"name"`
	Subscribers  int    `json:"subscribers"`
	Partitions   int    `json:"partitions"`
	HasTransform bool   `json:"has_transform"`
	DLQTopic     string `json:"dlq_topic,omitempty"`
}

// ListTopics returns metadata about all known topics.
func (e *BrokerEngine) ListTopics() []TopicInfo {
	e.mu.RLock()
	defer e.mu.RUnlock()

	seen := make(map[string]bool)
	var topics []TopicInfo

	for name, subs := range e.topics {
		info := TopicInfo{
			Name:        name,
			Subscribers: len(subs),
		}
		if parts, ok := e.partitions[name]; ok {
			info.Partitions = len(parts)
		}
		if _, ok := e.transforms[name]; ok {
			info.HasTransform = true
		}
		if dlq, ok := e.dlqTopics[name]; ok {
			info.DLQTopic = dlq
		}
		topics = append(topics, info)
		seen[name] = true
	}

	// Include topics that only have transforms/DLQ but no subscribers
	for name := range e.transforms {
		if !seen[name] {
			info := TopicInfo{Name: name, HasTransform: true}
			if dlq, ok := e.dlqTopics[name]; ok {
				info.DLQTopic = dlq
			}
			topics = append(topics, info)
			seen[name] = true
		}
	}

	for name := range e.topicQueues {
		if !seen[name] {
			info := TopicInfo{Name: name}
			topics = append(topics, info)
			seen[name] = true
		}
	}

	e.schemasMu.RLock()
	for name := range e.schemas {
		if !seen[name] {
			info := TopicInfo{Name: name}
			topics = append(topics, info)
			seen[name] = true
		}
	}
	e.schemasMu.RUnlock()

	return topics
}

func (e *BrokerEngine) SetRaftNode(node interface{}) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.raftNode = node
}

func (e *BrokerEngine) RegisterSchema(ctx context.Context, topic string, schema map[string]string) {
	e.schemasMu.Lock()
	if e.schemas == nil {
		e.schemas = make(map[string]map[string]string)
	}
	e.schemas[topic] = schema
	e.schemasMu.Unlock()

	if ctx.Value("replicated") == nil {
		e.mu.Lock()
		rn := e.raftNode
		e.mu.Unlock()
		if rn != nil {
			if raftNode, ok := rn.(*RaftNode); ok {
				raftNode.Replicate("REGISTER_SCHEMA", topic, nil, "", schema)
			}
		}
	}
}

func (e *BrokerEngine) GetSchema(topic string) (map[string]string, bool) {
	e.schemasMu.RLock()
	defer e.schemasMu.RUnlock()
	schema, ok := e.schemas[topic]
	return schema, ok
}

func (e *BrokerEngine) SetCompacted(topic string, compacted bool) {
	e.compactedTopicsMu.Lock()
	defer e.compactedTopicsMu.Unlock()
	if e.compactedTopics == nil {
		e.compactedTopics = make(map[string]bool)
	}
	e.compactedTopics[topic] = compacted
}

func (e *BrokerEngine) IsCompacted(topic string) bool {
	e.compactedTopicsMu.RLock()
	defer e.compactedTopicsMu.RUnlock()
	return e.compactedTopics[topic]
}

// SetDLQ registers a dead letter queue topic for a source topic.
// When a WASM transform fails for the source topic, the original
// payload is routed to the DLQ topic instead of being silently dropped.
func (e *BrokerEngine) SetDLQ(ctx context.Context, topic string, dlqTopic string) {
	e.mu.Lock()
	e.dlqTopics[topic] = dlqTopic
	rn := e.raftNode
	e.mu.Unlock()

	if ctx.Value("replicated") == nil && rn != nil {
		if raftNode, ok := rn.(*RaftNode); ok {
			raftNode.Replicate("SET_DLQ", topic, nil, dlqTopic, nil)
		}
	}
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

// SubscribeGroup registers a subscriber channel associated with a consumer group for a topic
func (e *BrokerEngine) SubscribeGroup(topic string, groupName string) chan string {
	e.mu.Lock()
	defer e.mu.Unlock()

	ch := make(chan string, 100)
	if e.groupSubs[topic] == nil {
		e.groupSubs[topic] = make(map[string][]chan string)
	}
	if e.groupIndices[topic] == nil {
		e.groupIndices[topic] = make(map[string]uint64)
	}
	e.groupSubs[topic][groupName] = append(e.groupSubs[topic][groupName], ch)
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

	groupsMap, hasGroup := e.groupSubs[topic]
	if hasGroup {
		for groupName, chs := range groupsMap {
			for i, sub := range chs {
				if sub == ch {
					e.groupSubs[topic][groupName] = append(chs[:i], chs[i+1:]...)
					if len(e.groupSubs[topic][groupName]) == 0 {
						delete(e.groupSubs[topic], groupName)
					}
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
	if len(wasmBytes) == 0 {
		if compiled, exists := e.transforms[topic]; exists {
			_ = compiled.Close(ctx)
			delete(e.transforms, topic)
		}
		rn := e.raftNode
		e.mu.Unlock()
		if ctx.Value("replicated") == nil && rn != nil {
			if raftNode, ok := rn.(*RaftNode); ok {
				raftNode.Replicate("REGISTER_TRANSFORM", topic, nil, "", nil)
			}
		}
		return nil
	}
	e.mu.Unlock()

	compiled, err := e.wasmManager.Compile(ctx, wasmBytes)
	if err != nil {
		return fmt.Errorf("failed to compile WASM module: %w", err)
	}

	e.mu.Lock()
	if old, exists := e.transforms[topic]; exists {
		go func(c wazero.CompiledModule) {
			time.Sleep(5 * time.Second)
			_ = c.Close(context.Background())
		}(old)
	}

	e.transforms[topic] = compiled
	rn := e.raftNode
	e.mu.Unlock()

	if ctx.Value("replicated") == nil && rn != nil {
		if raftNode, ok := rn.(*RaftNode); ok {
			raftNode.Replicate("REGISTER_TRANSFORM", topic, wasmBytes, "", nil)
		}
	}
	return nil
}

// PublishPartition hashes the key to dispatch messages to a specific partitioned queue
func (e *BrokerEngine) PublishPartition(ctx context.Context, topic string, key string, payload string) (string, error) {
	// Backpressure check
	pq := e.getOrCreateQueue(topic)
	limit := 1000
	if limitStr := os.Getenv("SERVQUEUE_BACKPRESSURE_LIMIT"); limitStr != "" {
		if parsed, err := strconv.Atoi(limitStr); err == nil && parsed > 0 {
			limit = parsed
		}
	}
	if pq.Len() >= limit {
		return "", fmt.Errorf("queue capacity exceeded: backpressure active")
	}

	if schema, ok := e.GetSchema(topic); ok {
		if err := ValidatePayload(payload, schema); err != nil {
			return payload, fmt.Errorf("schema validation failed: %w", err)
		}
	}

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

	priority := 0
	if prioVal, ok := ctx.Value("priority").(int); ok {
		priority = prioVal
	} else if prioStr, ok := ctx.Value("priority").(string); ok {
		if p, err := strconv.Atoi(prioStr); err == nil {
			priority = p
		}
	}

	expiry := getExpiryFromContext(ctx)
	if e.IsCompacted(topic) && key != "" {
		pq.PushWithKey(processed, key, priority, partitionId, expiry)
		pq.Compact()
	} else {
		pq.Push(processed, priority, partitionId, expiry)
	}

	otel.EndSpan(span, nil, map[string]interface{}{})
	return processed, nil
}

// Publish writes a message to a topic, running any registered WASM transform first
func (e *BrokerEngine) Publish(ctx context.Context, topic string, payload string) (string, error) {
	if e.publishLimiter != nil && !e.publishLimiter.Allow() {
		return "", fmt.Errorf("rate limit exceeded")
	}

	// Backpressure check
	pq := e.getOrCreateQueue(topic)
	limit := 1000
	if limitStr := os.Getenv("SERVQUEUE_BACKPRESSURE_LIMIT"); limitStr != "" {
		if parsed, err := strconv.Atoi(limitStr); err == nil && parsed > 0 {
			limit = parsed
		}
	}
	if pq.Len() >= limit {
		return "", fmt.Errorf("queue capacity exceeded: backpressure active")
	}

	if schema, ok := e.GetSchema(topic); ok {
		if err := ValidatePayload(payload, schema); err != nil {
			return payload, fmt.Errorf("schema validation failed: %w", err)
		}
	}

	if prodID, ok := ctx.Value("producer-id").(string); ok && prodID != "" {
		if seqVal := ctx.Value("sequence-number"); seqVal != nil {
			var seqNum int64
			var err error
			if sInt, ok := seqVal.(int64); ok {
				seqNum = sInt
			} else if sInt, ok := seqVal.(int); ok {
				seqNum = int64(sInt)
			} else if sStr, ok := seqVal.(string); ok {
				seqNum, err = strconv.ParseInt(sStr, 10, 64)
			}
			if err == nil {
				e.producerMu.Lock()
				lastSeq, exists := e.producerSequences[prodID]
				if exists && seqNum <= lastSeq {
					e.producerMu.Unlock()
					log.Printf("Broker: duplicate message detected for producer-id %s seq %d (last %d). Dropping but returning success.", prodID, seqNum, lastSeq)
					return payload, nil
				}
				e.producerSequences[prodID] = seqNum
				e.producerMu.Unlock()
			}
		}
	}

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

	if delayVal, ok := ctx.Value("delay-ms").(string); ok && delayVal != "" {
		if delayMs, err := strconv.Atoi(delayVal); err == nil && delayMs > 0 {
			msgID, _ := ctx.Value("message-id").(string)
			if msgID == "" {
				msgID = fmt.Sprintf("msg-%d-%d", time.Now().UnixNano(), atomic.AddUint64(&e.delayedCounter, 1))
			}
			targetTime := time.Now().Add(time.Duration(delayMs) * time.Millisecond)

			e.delayedMu.Lock()
			e.delayedMsgs[msgID] = DelayedMessage{
				ID:         msgID,
				Topic:      topic,
				Payload:    payload,
				TargetTime: targetTime,
			}
			e.delayedMu.Unlock()

			e.timeWheel.AddJob(time.Duration(delayMs)*time.Millisecond, func() {
				e.delayedMu.Lock()
				delete(e.delayedMsgs, msgID)
				e.delayedMu.Unlock()
				_, _ = e.publishLocal(context.Background(), topic, payload)
			})
			return payload, nil
		}
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

	priority := 0
	if prioVal, ok := ctx.Value("priority").(int); ok {
		priority = prioVal
	} else if prioStr, ok := ctx.Value("priority").(string); ok {
		if p, err := strconv.Atoi(prioStr); err == nil {
			priority = p
		}
	}

	expiry := getExpiryFromContext(ctx)
	msgKey, _ := ctx.Value("message-key").(string)
	if e.IsCompacted(topic) && msgKey != "" {
		pq.PushWithKey(processed, msgKey, priority, 0, expiry)
		pq.Compact()
	} else {
		pq.Push(processed, priority, 0, expiry)
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
	var subs []chan string
	for pattern, patternSubs := range e.topics {
		if matchTopic(pattern, topic) {
			subs = append(subs, patternSubs...)
		}
	}
	groupsMap := make(map[string][]chan string)
	for pattern, gSubsMap := range e.groupSubs {
		if matchTopic(pattern, topic) {
			for gName, gSubs := range gSubsMap {
				groupsMap[gName] = append(groupsMap[gName], gSubs...)
			}
		}
	}
	e.mu.RUnlock()

	for _, sub := range subs {
		select {
		case sub <- processed:
		default:
		}
	}

	for gName, gSubs := range groupsMap {
		if len(gSubs) == 0 {
			continue
		}
		e.mu.Lock()
		if e.groupIndices[topic] == nil {
			e.groupIndices[topic] = make(map[string]uint64)
		}
		idx := e.groupIndices[topic][gName]
		e.groupIndices[topic][gName] = idx + 1
		e.mu.Unlock()

		targetChan := gSubs[idx%uint64(len(gSubs))]
		select {
		case targetChan <- processed:
		default:
		}
	}

	otel.EndSpan(span, nil, map[string]interface{}{})
	return processed, nil
}

func (e *BrokerEngine) GetWALEntries() ([]storage.LogEntry, error) {
	if e.wal == nil {
		return []storage.LogEntry{}, nil
	}
	return e.wal.Recover()
}

func (e *BrokerEngine) GetDelayedMessages() []DelayedMessage {
	e.delayedMu.Lock()
	defer e.delayedMu.Unlock()

	msgs := make([]DelayedMessage, 0, len(e.delayedMsgs))
	for _, m := range e.delayedMsgs {
		msgs = append(msgs, m)
	}
	return msgs
}

func (e *BrokerEngine) GetOffset(group, topic string) int64 {
	e.mu.RLock()
	defer e.mu.RUnlock()
	if e.groupOffsets[group] == nil {
		return 0
	}
	return e.groupOffsets[group][topic]
}

func (e *BrokerEngine) CommitOffset(group, topic string, offset int64) {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.groupOffsets[group] == nil {
		e.groupOffsets[group] = make(map[string]int64)
	}
	e.groupOffsets[group][topic] = offset
}

func (e *BrokerEngine) ReplayMessages(ctx context.Context, topic string, startOffset int64, groupName string) (int, error) {
	if e.wal == nil {
		return 0, fmt.Errorf("WAL is not initialized")
	}

	entries, err := e.wal.Recover()
	if err != nil {
		return 0, err
	}

	var filtered []storage.LogEntry
	for _, entry := range entries {
		if entry.Topic == topic {
			filtered = append(filtered, entry)
		}
	}

	if startOffset < 0 || startOffset >= int64(len(filtered)) {
		return 0, nil
	}

	count := 0
	e.mu.RLock()
	subs := e.topics[topic]
	var gSubs []chan string
	if groupName != "" && e.groupSubs[topic] != nil {
		gSubs = e.groupSubs[topic][groupName]
	}
	e.mu.RUnlock()

	for i := startOffset; i < int64(len(filtered)); i++ {
		payload := filtered[i].Payload
		processed := payload

		e.mu.RLock()
		compiledModule, hasTransform := e.transforms[topic]
		e.mu.RUnlock()

		if hasTransform && compiledModule != nil {
			if p, err := e.wasmManager.RunTransform(ctx, compiledModule, payload, ""); err == nil {
				processed = p
			}
		}

		if groupName != "" {
			if len(gSubs) > 0 {
				e.mu.Lock()
				if e.groupIndices[topic] == nil {
					e.groupIndices[topic] = make(map[string]uint64)
				}
				idx := e.groupIndices[topic][groupName]
				e.groupIndices[topic][groupName] = idx + 1
				e.mu.Unlock()

				targetChan := gSubs[idx%uint64(len(gSubs))]
				select {
				case targetChan <- processed:
					count++
				default:
				}
			}
		} else {
			for _, sub := range subs {
				select {
				case sub <- processed:
				default:
				}
			}
			count++
		}
	}

	return count, nil
}

func matchTopic(pattern, topic string) bool {
	if pattern == topic {
		return true
	}
	if pattern == "#" {
		return true
	}
	pParts := strings.Split(pattern, ".")
	tParts := strings.Split(topic, ".")

	for i := 0; i < len(pParts); i++ {
		pPart := pParts[i]
		if pPart == "#" {
			return true
		}
		if i >= len(tParts) {
			return false
		}
		if pPart == "*" {
			continue
		}
		if pPart != tParts[i] {
			return false
		}
	}
	return len(pParts) == len(tParts)
}

func getExpiryFromContext(ctx context.Context) time.Time {
	var ttlMs int
	if ttlVal := ctx.Value("ttl-ms"); ttlVal != nil {
		if t, ok := ttlVal.(int); ok {
			ttlMs = t
		} else if t, ok := ttlVal.(string); ok {
			if parsed, err := strconv.Atoi(t); err == nil {
				ttlMs = parsed
			}
		} else if t, ok := ttlVal.(int64); ok {
			ttlMs = int(t)
		}
	} else if ttlVal := ctx.Value("ttl"); ttlVal != nil {
		if t, ok := ttlVal.(int); ok {
			ttlMs = t
		} else if t, ok := ttlVal.(string); ok {
			if parsed, err := strconv.Atoi(t); err == nil {
				ttlMs = parsed
			}
		} else if t, ok := ttlVal.(int64); ok {
			ttlMs = int(t)
		}
	}

	if ttlMs > 0 {
		return time.Now().Add(time.Duration(ttlMs) * time.Millisecond)
	}
	return time.Time{}
}

