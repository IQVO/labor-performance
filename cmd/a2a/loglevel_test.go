package main

import (
	"context"
	"log/slog"
	"testing"
)

// TestNewJSONHandler_AcceptsWarningAliasCaseInsensitive pins gap fix D22:
// cmd/a2a parses LOG_LEVEL like every other binary of this repo
// (case-insensitive, "warning" is an alias of "warn"). It used to compare
// the raw string, so DEBUG, WARN and warning silently meant info.
func TestNewJSONHandler_AcceptsWarningAliasCaseInsensitive(t *testing.T) {
	ctx := context.Background()
	for _, level := range []string{"warn", "warning", "WARNING", "Warn"} {
		h := newJSONHandler(level)
		if h.Enabled(ctx, slog.LevelInfo) || !h.Enabled(ctx, slog.LevelWarn) {
			t.Fatalf("LOG_LEVEL=%q: want warn level", level)
		}
	}
	if h := newJSONHandler("DEBUG"); !h.Enabled(ctx, slog.LevelDebug) {
		t.Fatal("LOG_LEVEL=DEBUG: want debug level")
	}
	if h := newJSONHandler("ERROR"); h.Enabled(ctx, slog.LevelWarn) || !h.Enabled(ctx, slog.LevelError) {
		t.Fatal("LOG_LEVEL=ERROR: want error level")
	}
	if h := newJSONHandler("verbose"); !h.Enabled(ctx, slog.LevelInfo) || h.Enabled(ctx, slog.LevelDebug) {
		t.Fatal("unknown LOG_LEVEL: want the info fallback")
	}
}
