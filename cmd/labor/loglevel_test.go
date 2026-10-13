package main

import (
	"context"
	"log/slog"
	"testing"
)

// TestNewLogger_AcceptsWarningAlias pins gap fix D22: every binary accepts
// LOG_LEVEL=warning like warn (case-insensitive), instead of silently
// falling back to info.
func TestNewLogger_AcceptsWarningAlias(t *testing.T) {
	for _, level := range []string{"warn", "warning", "WARNING"} {
		l := newLogger(level)
		if l.Enabled(context.Background(), slog.LevelInfo) || !l.Enabled(context.Background(), slog.LevelWarn) {
			t.Fatalf("LOG_LEVEL=%q: want warn level", level)
		}
	}
}
