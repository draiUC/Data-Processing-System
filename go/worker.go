package main

import (
	"context"
	"errors"
	"strconv"
	"strings"
	"sync"
	"time"
)

const (
	maxAttempts  = 3
	queueTimeout = 400 * time.Millisecond
)

// worker pulls tasks until the queue is closed and drained (or ctx ends).
func worker(ctx context.Context, id int, q *TaskQueue, results chan<- Result,
	stats chan<- WorkerStats, wg *sync.WaitGroup, log *Logger) {

	name := "worker-" + strconv.Itoa(id)
	st := WorkerStats{Name: name}

	defer wg.Done() // runs last: signals main that this goroutine is finished
	defer func() {
		if r := recover(); r != nil { // a panic must not take the whole program down
			log.Error(name, "PANIC recovered: %v", r)
		}
		log.Info(name, "completed: succeeded=%d, failed=%d, retries=%d", st.Succeeded, st.Failed, st.Retries)
		stats <- st // buffered channel, never blocks
	}()

	log.Info(name, "started")
	for {
		if err := ctx.Err(); err != nil {
			log.Warn(name, "stopping early: %v", err)
			return
		}

		task, err := q.GetTask(queueTimeout)
		switch {
		case errors.Is(err, ErrQueueEmpty):
			log.Warn(name, "queue empty (no task within %v); still waiting", queueTimeout)
			continue
		case errors.Is(err, ErrQueueClosed):
			return // normal termination
		case err != nil:
			log.Error(name, "unexpected queue error: %v", err)
			return
		}

		res, retries := handleTask(ctx, name, task, log)
		st.Retries += retries
		if res.Status == "SUCCESS" {
			st.Succeeded++
		} else {
			st.Failed++
		}
		results <- res
	}
}

// handleTask processes one task, retrying transient failures up to maxAttempts.
func handleTask(ctx context.Context, name string, t Task, log *Logger) (Result, int) {
	start := time.Now()
	retries := 0
	for attempt := 1; ; attempt++ {
		out, err := processTask(ctx, t, attempt)
		ms := time.Since(start).Milliseconds()
		if err == nil {
			log.Info(name, "task %d OK (attempts=%d, %dms) -> %s", t.ID, attempt, ms, out[strings.Index(out, "sha256="):])
			return Result{t.ID, name, t.Payload, out, ms, attempt, "SUCCESS", ""}, retries
		}

		var pe *ProcessingError
		if errors.As(err, &pe) && pe.Transient && attempt < maxAttempts {
			retries++
			log.Warn(name, "task %d attempt %d failed: %s - retrying", t.ID, attempt, pe.Reason)
			continue
		}
		log.Error(name, "task %d FAILED after %d attempt(s): %v", t.ID, attempt, err)
		return Result{t.ID, name, t.Payload, "", ms, attempt, "FAILED", err.Error()}, retries
	}
}
