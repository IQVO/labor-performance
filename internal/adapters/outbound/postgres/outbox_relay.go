package postgres

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	kafkago "github.com/segmentio/kafka-go"

	outboundkafka "github.com/claudioed/labor-performance/internal/adapters/outbound/kafka"
)

// Sink is where OutboxRelay forwards drained messages — in production the
// kafka.RelaySink; in tests a recorder.
type Sink interface {
	Send(ctx context.Context, msgs ...outboundkafka.Encoded) error
}

// OutboxRelay drains unpublished outbox_events rows onto a Sink, oldest
// first, marking each row published as it goes (ADR 0010).
//
// Delivery is at-least-once: a crash between a successful Send and the
// row's UPDATE republishes that row on the next pass. The analytics
// projector already dedupes on the envelope's event_id (which is part of
// the stored value, so a redelivery carries the SAME id), so a duplicate
// is a no-op for it. Ordering per partition key is preserved because rows
// are drained in id order within one relay and FOR UPDATE SKIP LOCKED
// keeps two relays (a rolling deploy's overlapping old and new pod) from
// claiming the same row.
type OutboxRelay struct {
	pool      *pgxpool.Pool
	sink      Sink
	logger    *slog.Logger
	interval  time.Duration
	batchSize int
}

// RelayOption customises an OutboxRelay.
type RelayOption func(*OutboxRelay)

// WithInterval sets how long the relay sleeps between passes when the
// last pass found nothing to publish. Default 1s.
func WithInterval(d time.Duration) RelayOption {
	return func(r *OutboxRelay) { r.interval = d }
}

// WithBatchSize caps how many rows one pass claims. Default 100.
func WithBatchSize(n int) RelayOption {
	return func(r *OutboxRelay) { r.batchSize = n }
}

// NewOutboxRelay constructs a relay draining pool into sink.
func NewOutboxRelay(pool *pgxpool.Pool, sink Sink, logger *slog.Logger, opts ...RelayOption) *OutboxRelay {
	if logger == nil {
		logger = slog.Default()
	}
	r := &OutboxRelay{pool: pool, sink: sink, logger: logger, interval: time.Second, batchSize: 100}
	for _, o := range opts {
		o(r)
	}
	return r
}

// Run drains the outbox until ctx is cancelled. A pass that publishes a
// full batch is followed immediately by another pass (there is probably
// more waiting); an empty pass sleeps for the configured interval. A
// failing pass is logged and retried after the interval — the rows stay
// unpublished, so nothing is lost.
func (r *OutboxRelay) Run(ctx context.Context) error {
	for {
		n, err := r.RelayOnce(ctx)
		if err != nil && !errors.Is(err, context.Canceled) {
			r.logger.ErrorContext(ctx, "outbox relay pass failed", "error", err)
		}
		if n == r.batchSize && err == nil {
			continue
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(r.interval):
		}
	}
}

// RelayOnce performs a single pass: claim up to batchSize unpublished rows
// under a row lock, send each in id order, and mark it published. It
// returns how many rows were published. Rows are sent ONE AT A TIME so
// that on the first Send failure the pass stops exactly at the failed row
// (preserving per-key ordering — a later event for the same key must not
// overtake a failed earlier one), records the error on that row, commits
// what was already sent, and returns the error.
func (r *OutboxRelay) RelayOnce(ctx context.Context) (int, error) {
	tx, err := r.pool.Begin(ctx)
	if err != nil {
		return 0, fmt.Errorf("postgres: begin relay pass: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	batch, err := claimOutboxRows(ctx, tx, r.batchSize)
	if err != nil {
		return 0, err
	}
	return r.publishBatch(ctx, tx, batch)
}

// pendingOutboxRow is one claimed, not-yet-published outbox_events row.
type pendingOutboxRow struct {
	id  int64
	msg outboundkafka.Encoded
}

// claimOutboxRows locks and reads up to batchSize unpublished rows, oldest
// first, for the pass to send. FOR UPDATE SKIP LOCKED keeps two relays (a
// rolling deploy's overlapping old and new pod) from claiming the same
// row.
func claimOutboxRows(ctx context.Context, tx pgx.Tx, batchSize int) ([]pendingOutboxRow, error) {
	rows, err := tx.Query(ctx, `
		SELECT id, topic, event_type, key, value, headers
		FROM outbox_events
		WHERE published_at IS NULL
		ORDER BY id
		LIMIT $1
		FOR UPDATE SKIP LOCKED
	`, batchSize)
	if err != nil {
		return nil, fmt.Errorf("postgres: claim outbox rows: %w", err)
	}
	var batch []pendingOutboxRow
	for rows.Next() {
		var p pendingOutboxRow
		var headers []byte
		if err := rows.Scan(&p.id, &p.msg.Topic, &p.msg.EventType, &p.msg.Key, &p.msg.Value, &headers); err != nil {
			rows.Close()
			return nil, fmt.Errorf("postgres: scan outbox row: %w", err)
		}
		if p.msg.Headers, err = unmarshalHeaders(headers); err != nil {
			rows.Close()
			return nil, fmt.Errorf("postgres: decode outbox row %d headers: %w", p.id, err)
		}
		batch = append(batch, p)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("postgres: read outbox rows: %w", err)
	}
	return batch, nil
}

// publishBatch sends each claimed row to the sink in id order, marking it
// published as it goes, then commits the pass. On the first Send failure it
// stops exactly at the failed row (preserving per-key ordering — a later
// event for the same key must not overtake a failed earlier one), records
// the error on that row, commits what was already sent, and returns the
// error: at-least-once, with the failed row retried on the next pass.
func (r *OutboxRelay) publishBatch(ctx context.Context, tx pgx.Tx, batch []pendingOutboxRow) (int, error) {
	published := 0
	for _, p := range batch {
		if err := r.sink.Send(ctx, p.msg); err != nil {
			return published, recordSendFailure(ctx, tx, p, err)
		}
		if err := markPublished(ctx, tx, p.id); err != nil {
			return published, err
		}
		published++
	}
	if err := tx.Commit(ctx); err != nil && !errors.Is(err, pgx.ErrTxClosed) {
		return published, fmt.Errorf("postgres: commit relay pass: %w", err)
	}
	if published > 0 {
		r.logger.DebugContext(ctx, "outbox relay published events", "count", published)
	}
	return published, nil
}

// recordSendFailure marks the failed row's attempt and error, commits the
// pass (persisting both the failure and every earlier row's published
// mark), and wraps err exactly as the relay has always reported it.
func recordSendFailure(ctx context.Context, tx pgx.Tx, p pendingOutboxRow, err error) error {
	if _, uerr := tx.Exec(ctx, `
		UPDATE outbox_events SET attempts = attempts + 1, last_error = $2 WHERE id = $1
	`, p.id, err.Error()); uerr != nil {
		err = errors.Join(err, fmt.Errorf("postgres: record outbox failure: %w", uerr))
	}
	if cerr := tx.Commit(ctx); cerr != nil {
		err = errors.Join(err, fmt.Errorf("postgres: commit relay pass: %w", cerr))
	}
	return fmt.Errorf("outbox relay: send %s to %s (row %d): %w", p.msg.EventType, p.msg.Topic, p.id, err)
}

// markPublished marks one successfully sent row published.
func markPublished(ctx context.Context, tx pgx.Tx, id int64) error {
	if _, err := tx.Exec(ctx, `
		UPDATE outbox_events SET published_at = now(), attempts = attempts + 1, last_error = NULL WHERE id = $1
	`, id); err != nil {
		return fmt.Errorf("postgres: mark outbox row %d published: %w", id, err)
	}
	return nil
}

func unmarshalHeaders(raw []byte) ([]kafkago.Header, error) {
	if len(raw) == 0 {
		return []kafkago.Header{}, nil
	}
	var hs []outboxHeader
	if err := json.Unmarshal(raw, &hs); err != nil {
		return nil, err
	}
	out := make([]kafkago.Header, 0, len(hs))
	for _, h := range hs {
		out = append(out, kafkago.Header{Key: h.Key, Value: []byte(h.Value)})
	}
	return out, nil
}
