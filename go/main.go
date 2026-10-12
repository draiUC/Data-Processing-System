// Multi-threaded Data Processing System (Go version).
//
// Usage: go run . [-workers 4] [-tasks 30] [-out output/go-results.csv] [-timeout 60s]
package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"sort"
	"sync"
	"time"
)

func main() {
	// run() returns the exit code so that its deferred cleanups execute before os.Exit.
	os.Exit(run())
}

func run() int {
	workers := flag.Int("workers", 4, "number of worker goroutines")
	taskCount := flag.Int("tasks", 30, "number of tasks to generate")
	outPath := flag.String("out", "output/go-results.csv", "CSV file for results")
	timeout := flag.Duration("timeout", 60*time.Second, "overall deadline for the run")
	flag.Parse()
	if *workers < 1 || *taskCount < 1 {
		fmt.Fprintln(os.Stderr, "workers and tasks must be positive")
		return 2
	}

	log, closeLog, logErr := NewLogger("logs/go-processing.log")
	defer closeLog()
	if logErr != nil {
		log.Warn("main", "cannot open log file (%v); logging to console only", logErr)
	}

	start := time.Now()
	ctx, cancel := context.WithTimeout(context.Background(), *timeout)
	defer cancel()

	// Output resource: fall back to memory-only if the file cannot be created.
	shownOut := *outPath
	sink, err := NewResultSink(*outPath)
	if err != nil {
		shownOut = "none (in-memory only)"
		log.Error("main", "cannot open output file %s: %v; falling back to in-memory results only", *outPath, err)
		sink = NewMemorySink()
	}
	defer sink.Close() // safety net; Close is idempotent

	queue := NewTaskQueue(10)
	resultsCh := make(chan Result)
	collected := make(chan []Result, 1)
	statsCh := make(chan WorkerStats, *workers)

	go collect(resultsCh, sink, log, collected)

	var wg sync.WaitGroup
	for i := 1; i <= *workers; i++ {
		wg.Add(1)
		go worker(ctx, i, queue, resultsCh, statsCh, &wg, log)
	}

	producerDone := make(chan struct{})
	go produce(ctx, queue, *taskCount, log, producerDone)

	wg.Wait()      // every worker has exited (queue drained, or deadline hit)
	<-producerDone // producer has finished and closed the queue
	close(resultsCh)
	results := <-collected
	close(statsCh)

	if err := sink.Close(); err != nil {
		log.Error("main", "error closing output file: %v", err)
	}
	return summarize(log, results, statsCh, *taskCount, shownOut, time.Since(start))
}

// produce enqueues the tasks, pausing once to simulate a slow upstream source.
func produce(ctx context.Context, q *TaskQueue, n int, log *Logger, done chan<- struct{}) {
	defer close(done)
	defer q.Close() // always close, or workers would wait forever

	log.Info("producer", "started; enqueueing %d tasks", n)
	for id := 1; id <= n; id++ {
		if err := q.AddTask(ctx, NewTask(id)); err != nil {
			log.Error("producer", "could not add task %d: %v", id, err)
			return
		}
		if n >= 4 && id == n/2 {
			log.Info("producer", "simulating slow upstream (pausing 900ms)")
			select {
			case <-time.After(900 * time.Millisecond):
			case <-ctx.Done():
				log.Error("producer", "cancelled: %v", ctx.Err())
				return
			}
		}
	}
	log.Info("producer", "all tasks enqueued")
}

func summarize(log *Logger, results []Result, stats <-chan WorkerStats, expected int,
	out string, elapsed time.Duration) int {

	sort.Slice(results, func(i, j int) bool { return results[i].TaskID < results[j].TaskID })
	ok := 0
	complete := len(results) == expected
	for i, r := range results {
		if r.Status == "SUCCESS" {
			ok++
		}
		if r.TaskID != i+1 { // no gaps, no duplicates
			complete = false
		}
	}

	var all []WorkerStats
	for s := range stats {
		all = append(all, s)
	}
	sort.Slice(all, func(i, j int) bool { return all[i].Name < all[j].Name })

	log.Info("main", "---------------- SUMMARY ----------------")
	log.Info("main", "tasks submitted : %d", expected)
	log.Info("main", "results saved   : %d (%d succeeded, %d failed)", len(results), ok, len(results)-ok)
	for _, s := range all {
		log.Info("main", "%s handled %d tasks, %d retries", s.Name, s.Succeeded+s.Failed, s.Retries)
	}
	if complete {
		log.Info("main", "integrity check : PASSED (no missing or duplicate tasks)")
	} else {
		log.Error("main", "integrity check : FAILED")
	}
	log.Info("main", "elapsed         : %d ms; output file: %s", elapsed.Milliseconds(), out)
	if !complete {
		return 1
	}
	return 0
}
