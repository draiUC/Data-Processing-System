package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"math/rand"
	"strings"
	"time"
)

// processTask is the "computational work": it waits to simulate load, then
// transforms and fingerprints the payload. The wait is cancellable via ctx
// (Go's counterpart to Java's InterruptedException).
func processTask(ctx context.Context, t Task, attempt int) (string, error) {
	delay := time.Duration(80+rand.Intn(141)) * time.Millisecond
	select {
	case <-time.After(delay):
	case <-ctx.Done():
		return "", ctx.Err()
	}

	// Deterministic simulated faults so error handling is visible in every run.
	switch {
	case t.ID%10 == 0:
		return "", &ProcessingError{TaskID: t.ID, Reason: "corrupt record (checksum mismatch)"}
	case t.ID%7 == 0 && attempt == 1:
		return "", &ProcessingError{TaskID: t.ID, Reason: "transient I/O timeout", Transient: true}
	}

	sum := sha256.Sum256([]byte(t.Payload))
	return fmt.Sprintf("%s|sha256=%s", strings.ToUpper(t.Payload), hex.EncodeToString(sum[:4])), nil
}
