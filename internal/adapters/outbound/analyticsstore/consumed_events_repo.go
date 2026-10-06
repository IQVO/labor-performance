package analyticsstore

import (
	"context"

	"github.com/jackc/pgx/v5/pgxpool"
)

// ConsumedEventsRepo is a pgxpool-backed consumer idempotency gate over the
// analytical database's analytics_consumed_events table. It structurally
// satisfies the analytics consumer's ProcessedEvents port, kept separate
// from the projection's own event-id claim so the two do not race for the
// same row: the consumer gate admits the event, the projection then records
// its effect.
type ConsumedEventsRepo struct {
	pool *pgxpool.Pool
}

// NewConsumedEventsRepo constructs a ConsumedEventsRepo over pool.
func NewConsumedEventsRepo(pool *pgxpool.Pool) *ConsumedEventsRepo {
	return &ConsumedEventsRepo{pool: pool}
}

// MarkProcessed records eventId in analytics_consumed_events if absent,
// returning true iff this call newly recorded it. Inside a UnitOfWork it
// joins the ctx-bound transaction, so a later failure in the same unit
// rolls the claim back.
func (r *ConsumedEventsRepo) MarkProcessed(ctx context.Context, eventId string) (bool, error) {
	tag, err := querierFrom(ctx, r.pool).Exec(ctx,
		`INSERT INTO analytics_consumed_events (event_id) VALUES ($1) ON CONFLICT (event_id) DO NOTHING`,
		eventId)
	if err != nil {
		return false, err
	}
	return tag.RowsAffected() > 0, nil
}
