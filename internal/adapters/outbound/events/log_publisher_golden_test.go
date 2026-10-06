package events_test

import (
	"bytes"
	"context"
	"encoding/json"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/claudioed/labor-performance/internal/adapters/outbound/events"
	"github.com/claudioed/labor-performance/internal/domain/shared"
)

// Golden wire-shape tests for the log publisher's JSON payload. The
// payload shape (key names, order, null handling, time format) is owned by
// this adapter, not by the domain event structs; these goldens were
// captured from the pre-refactor output (domain structs marshalled
// directly) and pin it byte-for-byte.

func goldenAt(hour int) time.Time {
	return time.Date(2026, 9, 5, hour, 0, 0, 0, time.UTC)
}

func goldenCases() map[string]shared.DomainEvent {
	travel := int64(15)
	pct := 86.5
	idle := int64(12)
	return map[string]shared.DomainEvent{
		"log_standard_defined.json": shared.NewLaborStandardDefined(
			goldenAt(9), "std-1", shared.Pick, 45, &travel, goldenAt(8)),
		"log_standard_revised.json": shared.NewLaborStandardRevised(
			goldenAt(10), "std-2", shared.Pack, 45, 40, nil, goldenAt(10)),
		"log_task_performance_recorded.json": shared.NewTaskPerformanceRecorded(
			goldenAt(11), "task-1", "assoc-1", shared.Pick, 52, &pct, &idle, goldenAt(9)),
		"log_task_performance_recorded_nil.json": shared.NewTaskPerformanceRecorded(
			goldenAt(11), "task-2", "", shared.Pack, 30, nil, nil, goldenAt(9).Add(30*time.Minute)),
	}
}

func TestLogPublisherPayloadMatchesGolden(t *testing.T) {
	for file, event := range goldenCases() {
		t.Run(file, func(t *testing.T) {
			var buf bytes.Buffer
			pub := events.NewLogPublisher(slog.New(slog.NewJSONHandler(&buf, nil)))
			if err := pub.Publish(context.Background(), event); err != nil {
				t.Fatalf("Publish: %v", err)
			}

			var line struct {
				EventName string          `json:"event_name"`
				Payload   json.RawMessage `json:"payload"`
			}
			if err := json.Unmarshal(buf.Bytes(), &line); err != nil {
				t.Fatalf("log line is not JSON: %v\n%s", err, buf.String())
			}
			if line.EventName != event.EventName() {
				t.Errorf("event_name = %q, want %q", line.EventName, event.EventName())
			}

			want, err := os.ReadFile(filepath.Join("testdata", file))
			if err != nil {
				t.Fatalf("read golden: %v", err)
			}
			if got, w := string(line.Payload), strings.TrimSpace(string(want)); got != w {
				t.Errorf("payload mismatch\n got: %s\nwant: %s", got, w)
			}
		})
	}
}
