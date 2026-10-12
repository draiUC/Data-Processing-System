package main

import (
	"encoding/csv"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
)

// ResultSink writes results to a CSV file. It is used by exactly one goroutine
// (the collector), so it needs no mutex: ownership, not locking, gives safety.
type ResultSink struct {
	file *os.File
	w    *csv.Writer
}

// NewResultSink creates the output file and writes the header row.
func NewResultSink(path string) (*ResultSink, error) {
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return nil, fmt.Errorf("create output directory: %w", err)
	}
	f, err := os.Create(path)
	if err != nil {
		return nil, fmt.Errorf("create output file: %w", err)
	}
	s := &ResultSink{file: f, w: csv.NewWriter(f)}
	header := []string{"task_id", "worker", "input", "output", "duration_ms", "attempts", "status", "error"}
	if err := s.w.Write(header); err != nil {
		_ = f.Close()
		return nil, fmt.Errorf("write header: %w", err)
	}
	return s, nil
}

// NewMemorySink is the fallback used when the output file cannot be opened.
func NewMemorySink() *ResultSink { return &ResultSink{} }

// Write appends one row. After the first failure the file is closed and the
// sink silently becomes memory-only; the caller still holds every result.
func (s *ResultSink) Write(r Result) error {
	if s.file == nil {
		return nil
	}
	row := []string{strconv.Itoa(r.TaskID), r.Worker, r.Input, r.Output,
		strconv.FormatInt(r.DurationMs, 10), strconv.Itoa(r.Attempts), r.Status, r.Err}
	if err := s.w.Write(row); err != nil {
		_ = s.Close()
		return fmt.Errorf("write task %d: %w", r.TaskID, err)
	}
	s.w.Flush()
	if err := s.w.Error(); err != nil {
		_ = s.Close()
		return fmt.Errorf("flush task %d: %w", r.TaskID, err)
	}
	return nil
}

// Close flushes and closes the file. It is safe to call more than once.
func (s *ResultSink) Close() error {
	if s.file == nil {
		return nil
	}
	s.w.Flush()
	flushErr := s.w.Error()
	closeErr := s.file.Close()
	s.file, s.w = nil, nil
	if flushErr != nil {
		return flushErr
	}
	return closeErr
}

// collect is the single goroutine that receives every Result from the workers,
// drops duplicates, writes them to the sink and finally returns the full list.
func collect(in <-chan Result, sink *ResultSink, log *Logger, out chan<- []Result) {
	seen := make(map[int]bool)
	var all []Result
	for r := range in { // ends when main closes the channel after all workers exit
		if seen[r.TaskID] {
			log.Warn("collector", "duplicate result for task %d ignored", r.TaskID)
			continue
		}
		seen[r.TaskID] = true
		all = append(all, r)
		if err := sink.Write(r); err != nil {
			log.Error("collector", "file write failed: %v; continuing in memory only", err)
		}
	}
	out <- all
}
