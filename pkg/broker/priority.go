package broker

import (
	"container/heap"
	"sync"
	"time"
)

// PriorityMessage represents a message stored in the PriorityQueue.
type PriorityMessage struct {
	Payload     string
	Priority    int
	Sequence    uint64
	PartitionId int
	Index       int // Index of the item in the heap; maintained by heap.Interface
	Expiry      time.Time
}

type messageHeap []*PriorityMessage

func (h messageHeap) Len() int { return len(h) }
func (h messageHeap) Less(i, j int) bool {
	// Higher priority first
	if h[i].Priority != h[j].Priority {
		return h[i].Priority > h[j].Priority
	}
	// FIFO for equal priority (smaller sequence first)
	return h[i].Sequence < h[j].Sequence
}
func (h messageHeap) Swap(i, j int) {
	h[i], h[j] = h[j], h[i]
	h[i].Index = i
	h[j].Index = j
}
func (h *messageHeap) Push(x interface{}) {
	n := len(*h)
	item := x.(*PriorityMessage)
	item.Index = n
	*h = append(*h, item)
}
func (h *messageHeap) Pop() interface{} {
	old := *h
	n := len(old)
	item := old[n-1]
	old[n-1] = nil // avoid memory leak
	item.Index = -1
	*h = old[0 : n-1]
	return item
}

// PriorityQueue is a thread-safe priority queue wrapping a heap.
type PriorityQueue struct {
	mu       sync.Mutex
	h        messageHeap
	cond     *sync.Cond
	sequence uint64
}

// NewPriorityQueue creates and initializes a new PriorityQueue.
func NewPriorityQueue() *PriorityQueue {
	pq := &PriorityQueue{
		h: make(messageHeap, 0),
	}
	pq.cond = sync.NewCond(&pq.mu)
	return pq
}

// Push adds a message to the priority queue with the given priority and expiry.
func (pq *PriorityQueue) Push(payload string, priority int, partitionId int, expiry time.Time) {
	pq.mu.Lock()
	defer pq.mu.Unlock()

	pq.sequence++
	msg := &PriorityMessage{
		Payload:     payload,
		Priority:    priority,
		Sequence:    pq.sequence,
		PartitionId: partitionId,
		Expiry:      expiry,
	}
	heap.Push(&pq.h, msg)
	pq.cond.Signal()
}

// Pop blocks until a message is available, then removes and returns the highest priority message.
func (pq *PriorityQueue) Pop() *PriorityMessage {
	pq.mu.Lock()
	defer pq.mu.Unlock()

	for len(pq.h) == 0 {
		pq.cond.Wait()
	}

	msg := heap.Pop(&pq.h).(*PriorityMessage)
	return msg
}

// PopNonBlocking removes and returns the highest priority message if available, returning false otherwise.
func (pq *PriorityQueue) PopNonBlocking() (*PriorityMessage, bool) {
	pq.mu.Lock()
	defer pq.mu.Unlock()

	if len(pq.h) == 0 {
		return nil, false
	}

	msg := heap.Pop(&pq.h).(*PriorityMessage)
	return msg, true
}

// Len returns the current length of the queue.
func (pq *PriorityQueue) Len() int {
	pq.mu.Lock()
	defer pq.mu.Unlock()
	return len(pq.h)
}
