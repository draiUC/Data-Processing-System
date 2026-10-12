package main

import (
	"errors"
	"fmt"
)

// Task is a unit of work placed on the shared queue.
type Task struct {
	ID      int
	Payload string
}

// NewTask builds a deterministic sample record (identical to the Java version).
func NewTask(id int) Task {
	return Task{ID: id, Payload: fmt.Sprintf("sensor-%03d,reading=%d", id, (id*37+11)%100)}
}

// Result is the outcome of processing one task.
type Result struct {
	TaskID     int
	Worker     string
	Input      string
	Output     string
	DurationMs int64
	Attempts   int
	Status     string // "SUCCESS" or "FAILED"
	Err        string
}

// WorkerStats is sent by each worker to main when it exits.
type WorkerStats struct {
	Name      string
	Succeeded int
	Failed    int
	Retries   int
}

// Sentinel errors returned by the queue; callers test them with errors.Is.
var (
	ErrQueueEmpty  = errors.New("queue empty: no task arrived before the timeout")
	ErrQueueClosed = errors.New("queue closed and drained")
)

// ProcessingError describes a failure while processing one task.
type ProcessingError struct {
	TaskID    int
	Reason    string
	Transient bool // transient failures may be retried
}

func (e *ProcessingError) Error() string {
	return e.Reason
}
