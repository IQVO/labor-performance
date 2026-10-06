// Package events provides outbound EventPublisher implementations. v1
// ships only the log publisher — see CLAUDE.md's "Domain events" section:
// LaborStandardDefined/LaborStandardRevised/TaskPerformanceRecorded are
// published for symmetry with the fleet's convention and to leave an
// integration seam open, but no Kafka publish is required since no other
// repo consumes them yet.
package events

import (
	"context"
	"encoding/json"
	"log/slog"

	"github.com/claudioed/labor-performance/internal/domain/shared"
)

// LogPublisher publishes domain events by logging them as JSON.
type LogPublisher struct {
	logger *slog.Logger
}

// NewLogPublisher constructs a LogPublisher. A nil logger defaults to
// slog.Default().
func NewLogPublisher(logger *slog.Logger) *LogPublisher {
	if logger == nil {
		logger = slog.Default()
	}
	return &LogPublisher{logger: logger}
}

func (p *LogPublisher) Publish(ctx context.Context, events ...shared.DomainEvent) error {
	for _, event := range events {
		payload, err := json.Marshal(logPayload(event))
		if err != nil {
			return err
		}
		p.logger.InfoContext(ctx, "domain event published",
			"event_name", event.EventName(),
			"payload", json.RawMessage(payload),
		)
	}
	return nil
}
