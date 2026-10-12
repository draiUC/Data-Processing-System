package main

import (
	"context"
	"sync"
	"time"
)

// TaskQueue is a concurrency-safe FIFO queue built on a buffered channel.
// The channel does the locking; the RWMutex only protects the "closed" flag so
// that AddTask can never send on a closed channel (which would panic).
type TaskQueue struct {
	ch     chan Task
	mu     sync.RWMutex
	closed bool
}

// NewTaskQueue creates a queue that holds up to capacity pending tasks.
func NewTaskQueue(capacity int) *TaskQueue {
	return &TaskQueue{ch: make(chan Task, capacity)}
}

// AddTask enqueues a task. It blocks while the queue is full and gives up if
// ctx is cancelled or the queue has been closed.
func (q *TaskQueue) AddTask(ctx context.Context, t Task) error {
	q.mu.RLock()
	defer q.mu.RUnlock() // Close() waits for in-flight AddTask calls to finish
	if q.closed {
		return ErrQueueClosed
	}
	select {
	case q.ch <- t:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// GetTask waits up to timeout for the next task.
// It returns ErrQueueEmpty on timeout, and ErrQueueClosed once the queue is
// closed AND every remaining task has been received (the shutdown signal).
func (q *TaskQueue) GetTask(timeout time.Duration) (Task, error) {
	timer := time.NewTimer(timeout)
	defer timer.Stop()
	select {
	case t, ok := <-q.ch:
		if !ok {
			return Task{}, ErrQueueClosed
		}
		return t, nil
	case <-timer.C:
		return Task{}, ErrQueueEmpty
	}
}

// Close marks the queue finished. Buffered tasks can still be received.
func (q *TaskQueue) Close() {
	q.mu.Lock()
	defer q.mu.Unlock()
	if !q.closed {
		q.closed = true
		close(q.ch)
	}
}
