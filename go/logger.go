package main

import (
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"time"
)

// Logger writes timestamped, levelled lines to stdout and (if possible) a log file.
// log.Logger is safe for concurrent use, so goroutines can share one instance.
type Logger struct{ l *log.Logger }

// NewLogger returns the logger, a cleanup function, and a non-nil error if the
// log file could not be opened (the logger still works, console only).
func NewLogger(path string) (*Logger, func(), error) {
	cleanup := func() {}
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return &Logger{log.New(os.Stdout, "", 0)}, cleanup, err
	}
	f, err := os.Create(path)
	if err != nil {
		return &Logger{log.New(os.Stdout, "", 0)}, cleanup, err
	}
	cleanup = func() { _ = f.Close() }
	return &Logger{log.New(io.MultiWriter(os.Stdout, f), "", 0)}, cleanup, nil
}

func (lg *Logger) write(level, who, format string, args ...any) {
	lg.l.Printf("%s %-5s [%s] %s", time.Now().Format("15:04:05.000"), level, who,
		fmt.Sprintf(format, args...))
}

func (lg *Logger) Info(who, format string, args ...any)  { lg.write("INFO", who, format, args...) }
func (lg *Logger) Warn(who, format string, args ...any)  { lg.write("WARN", who, format, args...) }
func (lg *Logger) Error(who, format string, args ...any) { lg.write("ERROR", who, format, args...) }
