package main

import (
	"context"
	"testing"
	"time"
)

// TestHousekeepingDurationEnv pins gap fix D15: for the housekeeping knobs
// (HOUSEKEEPING_INTERVAL, IDEMPOTENCY_KEY_TTL, OUTBOX_RETENTION) a zero or
// negative duration is a real value that disables the sweeper or one of
// its halves, as ADR-0023, the sweeper's doc comment, startHousekeeping's
// doc comment and the chart say. It used to be swallowed by durationEnv's
// "<= 0 falls back" rule, so "0s" silently meant the 1h / 24h / 168h
// defaults.
func TestHousekeepingDurationEnv(t *testing.T) {
	const fallback = time.Hour
	tests := []struct {
		name string
		env  string
		want time.Duration
	}{
		{"unset falls back", "", fallback},
		{"zero disables", "0", 0},
		{"zero with unit disables", "0s", 0},
		{"negative disables", "-1s", -time.Second},
		{"positive is used", "30m", 30 * time.Minute},
		{"garbage falls back", "soon", fallback},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Setenv("HOUSEKEEPING_INTERVAL", tt.env)
			if got := housekeepingDurationEnv("HOUSEKEEPING_INTERVAL", fallback); got != tt.want {
				t.Fatalf("housekeepingDurationEnv(%q) = %v, want %v", tt.env, got, tt.want)
			}
		})
	}
}

// TestDurationEnv_RelayIntervalStillRejectsZero keeps OUTBOX_RELAY_INTERVAL
// on the strict helper: a 0 relay interval would be a busy loop, not a
// "disable".
func TestDurationEnv_RelayIntervalStillRejectsZero(t *testing.T) {
	t.Setenv("OUTBOX_RELAY_INTERVAL", "0s")
	if got := durationEnv("OUTBOX_RELAY_INTERVAL", time.Second); got != time.Second {
		t.Fatalf("durationEnv(0s) = %v, want the 1s fallback", got)
	}
}

// TestBuildSweeper_ZeroIntervalDisablesTheJob proves the wiring end to end:
// HOUSEKEEPING_INTERVAL=0 builds a sweeper whose Run returns at once
// without ever touching the pool (nil here: a sweep would panic).
func TestBuildSweeper_ZeroIntervalDisablesTheJob(t *testing.T) {
	t.Setenv("HOUSEKEEPING_INTERVAL", "0")
	sweeper := buildSweeper(nil, quietLogger())

	done := make(chan error, 1)
	go func() { done <- sweeper.Run(context.Background()) }()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Run = %v, want nil", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("HOUSEKEEPING_INTERVAL=0 must disable the sweeper, but Run kept running")
	}
}
