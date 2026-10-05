package postgres

import (
	"context"
	"errors"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

// fakeRow lets a unit test script pgx.Row.Scan's outcome without a real
// database — oldestUnpublishedLagSeconds only ever calls Scan on the
// single row QueryRow hands back.
type fakeRow struct {
	values []any
	err    error
}

func (r fakeRow) Scan(dest ...any) error {
	if r.err != nil {
		return r.err
	}
	if len(dest) != len(r.values) {
		return errors.New("fakeRow: scan arity mismatch")
	}
	for i, d := range dest {
		p, ok := d.(*float64)
		if !ok {
			return errors.New("fakeRow: unsupported scan target")
		}
		*p = r.values[i].(float64)
	}
	return nil
}

// fakeQuerier implements the querier interface for a unit test, scripting
// only QueryRow (the only method oldestUnpublishedLagSeconds calls).
type fakeQuerier struct{ row fakeRow }

func (f fakeQuerier) Exec(context.Context, string, ...any) (pgconn.CommandTag, error) {
	return pgconn.CommandTag{}, errors.New("fakeQuerier: Exec not implemented")
}
func (f fakeQuerier) Query(context.Context, string, ...any) (pgx.Rows, error) {
	return nil, errors.New("fakeQuerier: Query not implemented")
}
func (f fakeQuerier) QueryRow(context.Context, string, ...any) pgx.Row { return f.row }

func TestOldestUnpublishedLagSeconds_NoUnpublishedRows_ReturnsZero(t *testing.T) {
	q := fakeQuerier{row: fakeRow{err: pgx.ErrNoRows}}
	lag, err := oldestUnpublishedLagSeconds(context.Background(), q)
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	if lag != 0 {
		t.Fatalf("expected lag 0 when the outbox is drained, got %v", lag)
	}
}

func TestOldestUnpublishedLagSeconds_OldestRow_ReturnsItsAge(t *testing.T) {
	q := fakeQuerier{row: fakeRow{values: []any{42.5}}}
	lag, err := oldestUnpublishedLagSeconds(context.Background(), q)
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	if lag != 42.5 {
		t.Fatalf("expected lag 42.5, got %v", lag)
	}
}

func TestOldestUnpublishedLagSeconds_QueryError_IsWrapped(t *testing.T) {
	wantErr := errors.New("connection reset")
	q := fakeQuerier{row: fakeRow{err: wantErr}}
	_, err := oldestUnpublishedLagSeconds(context.Background(), q)
	if err == nil || !errors.Is(err, wantErr) {
		t.Fatalf("expected wrapped %v, got %v", wantErr, err)
	}
}
