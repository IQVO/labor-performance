package main

import (
	"log/slog"
	"os"
	"strconv"
	"strings"
	"time"
)

// durationEnv parses key as a time.Duration, falling back on absence or a
// malformed value (the relay interval is a tuning knob, not a contract, so
// a typo must not fail the boot). A zero or negative value also falls
// back: for OUTBOX_RELAY_INTERVAL it would be a busy loop, not a
// meaningful setting. The housekeeping knobs, where 0 means "disabled",
// use housekeepingDurationEnv instead.
func durationEnv(key string, fallback time.Duration) time.Duration {
	v := os.Getenv(key)
	if v == "" {
		return fallback
	}
	d, err := time.ParseDuration(v)
	if err != nil || d <= 0 {
		return fallback
	}
	return d
}

// housekeepingDurationEnv parses one of the housekeeping sweeper's knobs
// (ADR-0023). Unlike durationEnv, a zero or negative duration is returned
// as is, because the sweeper reads it as "disabled":
// HOUSEKEEPING_INTERVAL=0 turns the whole job off, IDEMPOTENCY_KEY_TTL=0 /
// OUTBOX_RETENTION=0 turn off that half (gap fix D15: the ADR, the chart
// and the sweeper's own doc comment always said so, but durationEnv
// silently replaced 0 with the default). Unset or unparseable values fall
// back.
func housekeepingDurationEnv(key string, fallback time.Duration) time.Duration {
	v := os.Getenv(key)
	if v == "" {
		return fallback
	}
	d, err := time.ParseDuration(v)
	if err != nil {
		return fallback
	}
	return d
}

// OUTBOX_MAX_ATTEMPTS caps how many failed sends an outbox row may
// accumulate before the relay stops claiming it (ADR 0032). 0 = unlimited.
const (
	outboxMaxAttemptsEnv     = "OUTBOX_MAX_ATTEMPTS"
	defaultOutboxMaxAttempts = 20
)

// nonNegativeIntEnv reads a non-negative integer from key, falling back to
// fallback for an unset value and, with a warning, for an unparseable or
// negative one. 0 is a valid value (OUTBOX_MAX_ATTEMPTS uses it for
// "unlimited").
func nonNegativeIntEnv(key string, fallback int, logger *slog.Logger) int {
	raw := os.Getenv(key)
	if raw == "" {
		return fallback
	}
	n, err := strconv.Atoi(strings.TrimSpace(raw))
	if err != nil || n < 0 {
		logger.Warn("ignoring invalid integer env var", "key", key, "value", raw, "fallback", fallback)
		return fallback
	}
	return n
}

func getenv(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

// idleGapCapSecondsEnv resolves IDLE_GAP_CAP_SECONDS, falling back to
// RecordTaskPerformance's own defaultIdleGapCapSeconds (3600) on absence
// or a malformed/non-positive value — a typo here must degrade
// gracefully, not crash the boot, mirroring durationEnv's discipline for
// OUTBOX_RELAY_INTERVAL.
func idleGapCapSecondsEnv() int64 {
	v := os.Getenv("IDLE_GAP_CAP_SECONDS")
	if v == "" {
		return 0
	}
	n, err := strconv.ParseInt(v, 10, 64)
	if err != nil || n <= 0 {
		return 0
	}
	return n
}
