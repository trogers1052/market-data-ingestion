package ratelimit

import (
	"context"
	"sync"
	"time"
)

// Limiter implements a token bucket rate limiter with request queuing
type Limiter struct {
	rate       float64       // requests per second
	burst      int           // max burst size
	tokens     float64       // current tokens available
	lastUpdate time.Time     // last token update time
	mu         sync.Mutex
	queue      chan struct{} // semaphore for queuing
}

// NewLimiter creates a new rate limiter
// rate: requests per second (e.g., 5 for 5 req/sec)
// burst: maximum burst size (tokens that can accumulate)
func NewLimiter(rate float64, burst int) *Limiter {
	return &Limiter{
		rate:       rate,
		burst:      burst,
		tokens:     float64(burst), // Start full
		lastUpdate: time.Now(),
		queue:      make(chan struct{}, burst*2), // Queue capacity
	}
}

// Wait blocks until a token is available or context is cancelled
func (l *Limiter) Wait(ctx context.Context) error {
	l.mu.Lock()

	// Refill tokens based on elapsed time
	now := time.Now()
	elapsed := now.Sub(l.lastUpdate).Seconds()
	l.tokens += elapsed * l.rate
	if l.tokens > float64(l.burst) {
		l.tokens = float64(l.burst)
	}
	l.lastUpdate = now

	// If we have a token, take it immediately
	if l.tokens >= 1 {
		l.tokens--
		l.mu.Unlock()
		return nil
	}

	// Calculate wait time for next token
	waitTime := time.Duration((1 - l.tokens) / l.rate * float64(time.Second))
	l.mu.Unlock()

	// Wait for token
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-time.After(waitTime):
		l.mu.Lock()
		l.tokens-- // Consume the token we waited for
		l.mu.Unlock()
		return nil
	}
}

// TryAcquire attempts to acquire a token without blocking
// Returns true if token was acquired, false otherwise
func (l *Limiter) TryAcquire() bool {
	l.mu.Lock()
	defer l.mu.Unlock()

	// Refill tokens
	now := time.Now()
	elapsed := now.Sub(l.lastUpdate).Seconds()
	l.tokens += elapsed * l.rate
	if l.tokens > float64(l.burst) {
		l.tokens = float64(l.burst)
	}
	l.lastUpdate = now

	if l.tokens >= 1 {
		l.tokens--
		return true
	}
	return false
}

// Available returns the current number of available tokens
func (l *Limiter) Available() float64 {
	l.mu.Lock()
	defer l.mu.Unlock()

	// Refill tokens
	now := time.Now()
	elapsed := now.Sub(l.lastUpdate).Seconds()
	tokens := l.tokens + elapsed*l.rate
	if tokens > float64(l.burst) {
		tokens = float64(l.burst)
	}
	return tokens
}

// PolygonLimiter is a specialized rate limiter for Polygon.io API
// Free tier: 5 requests/minute
// Starter tier: Unlimited (but we'll be respectful)
type PolygonLimiter struct {
	limiter *Limiter
}

// NewPolygonLimiter creates a rate limiter configured for Polygon.io
// tier: "free" (5/min), "starter" (100/min), "developer" (unlimited but throttled)
func NewPolygonLimiter(tier string) *PolygonLimiter {
	var rate float64
	var burst int

	switch tier {
	case "free":
		rate = 5.0 / 60.0  // 5 per minute = 0.083 per second
		burst = 5
	case "starter":
		rate = 100.0 / 60.0 // ~1.67 per second
		burst = 10
	case "developer", "advanced", "":
		rate = 10.0 // 10 per second (self-imposed limit)
		burst = 20
	default:
		rate = 5.0 // Conservative default
		burst = 5
	}

	return &PolygonLimiter{
		limiter: NewLimiter(rate, burst),
	}
}

// Wait blocks until a request can be made
func (p *PolygonLimiter) Wait(ctx context.Context) error {
	return p.limiter.Wait(ctx)
}

// TryAcquire attempts to acquire permission for a request without blocking
func (p *PolygonLimiter) TryAcquire() bool {
	return p.limiter.TryAcquire()
}

// RequestQueue manages a queue of pending requests with rate limiting
type RequestQueue struct {
	limiter  *Limiter
	pending  chan func(context.Context) error
	workers  int
	wg       sync.WaitGroup
	stopChan chan struct{}
}

// NewRequestQueue creates a new request queue with rate limiting
func NewRequestQueue(limiter *Limiter, workers int, queueSize int) *RequestQueue {
	return &RequestQueue{
		limiter:  limiter,
		pending:  make(chan func(context.Context) error, queueSize),
		workers:  workers,
		stopChan: make(chan struct{}),
	}
}

// Start begins processing queued requests
func (q *RequestQueue) Start(ctx context.Context) {
	for i := 0; i < q.workers; i++ {
		q.wg.Add(1)
		go q.worker(ctx)
	}
}

// worker processes requests from the queue
func (q *RequestQueue) worker(ctx context.Context) {
	defer q.wg.Done()

	for {
		select {
		case <-ctx.Done():
			return
		case <-q.stopChan:
			return
		case fn := <-q.pending:
			// Wait for rate limit
			if err := q.limiter.Wait(ctx); err != nil {
				continue
			}
			// Execute the request
			if err := fn(ctx); err != nil {
				// Log error but continue processing
				continue
			}
		}
	}
}

// Submit adds a request to the queue
// Returns error if queue is full
func (q *RequestQueue) Submit(fn func(context.Context) error) error {
	select {
	case q.pending <- fn:
		return nil
	default:
		return ErrQueueFull
	}
}

// Stop stops the request queue
func (q *RequestQueue) Stop() {
	close(q.stopChan)
	q.wg.Wait()
}

// QueueLength returns the number of pending requests
func (q *RequestQueue) QueueLength() int {
	return len(q.pending)
}

// ErrQueueFull is returned when the request queue is at capacity
var ErrQueueFull = &QueueFullError{}

type QueueFullError struct{}

func (e *QueueFullError) Error() string {
	return "request queue is full"
}
