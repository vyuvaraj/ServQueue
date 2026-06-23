package broker

import (
	"container/list"
	"sync"
	"time"
)

type twJob struct {
	rounds int
	task   func()
}

type TimeWheel struct {
	mu         sync.Mutex
	interval   time.Duration
	buckets    []*list.List
	currentPos int
	ticker     *time.Ticker
	stopChan   chan struct{}
}

// NewTimeWheel creates a new TimeWheel with the given tick interval and number of buckets.
func NewTimeWheel(interval time.Duration, numBuckets int) *TimeWheel {
	buckets := make([]*list.List, numBuckets)
	for i := 0; i < numBuckets; i++ {
		buckets[i] = list.New()
	}
	return &TimeWheel{
		interval:   interval,
		buckets:    buckets,
		currentPos: 0,
		stopChan:   make(chan struct{}),
	}
}

// Start starts the time-wheel ticker loop.
func (tw *TimeWheel) Start() {
	tw.ticker = time.NewTicker(tw.interval)
	go tw.run()
}

// Stop stops the time-wheel ticker loop.
func (tw *TimeWheel) Stop() {
	if tw.ticker != nil {
		tw.ticker.Stop()
	}
	close(tw.stopChan)
}

func (tw *TimeWheel) run() {
	for {
		select {
		case <-tw.ticker.C:
			tw.tick()
		case <-tw.stopChan:
			return
		}
	}
}

func (tw *TimeWheel) tick() {
	tw.mu.Lock()
	l := tw.buckets[tw.currentPos]
	var jobsToRun []func()
	for e := l.Front(); e != nil; {
		next := e.Next()
		j := e.Value.(*twJob)
		if j.rounds <= 0 {
			jobsToRun = append(jobsToRun, j.task)
			l.Remove(e)
		} else {
			j.rounds--
		}
		e = next
	}
	tw.currentPos = (tw.currentPos + 1) % len(tw.buckets)
	tw.mu.Unlock()

	for _, task := range jobsToRun {
		go task()
	}
}

// AddJob schedules a function to run after the specified delay.
func (tw *TimeWheel) AddJob(delay time.Duration, task func()) {
	tw.mu.Lock()
	defer tw.mu.Unlock()

	ticks := int(delay / tw.interval)
	if ticks < 1 {
		ticks = 1
	}

	rounds := ticks / len(tw.buckets)
	pos := (tw.currentPos + ticks) % len(tw.buckets)

	tw.buckets[pos].PushBack(&twJob{
		rounds: rounds,
		task:   task,
	})
}
