package analyticsstore

import (
	"context"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/claudioed/labor-performance/internal/pgtx"
)

// querier is the subset of pgx that both *pgxpool.Pool and pgx.Tx satisfy.
type querier interface {
	Exec(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error)
}

// querierFrom resolves the transaction bound to ctx, or falls back to the
// pool when the caller is not inside a UnitOfWork.
func querierFrom(ctx context.Context, pool *pgxpool.Pool) querier {
	if tx, ok := pgtx.TxFrom(ctx); ok {
		return tx
	}
	return pool
}

// UnitOfWork runs a function inside one transaction on the analytical
// database. The analytics consumer uses it so the consumer-level
// idempotency claim (ConsumedEventsRepo) and the projection apply
// (PostgresProjection) commit together or not at all: if the apply fails
// the claim is rolled back too, so the redelivered message is applied
// instead of being skipped as a duplicate.
type UnitOfWork struct {
	pool *pgxpool.Pool
}

// NewUnitOfWork constructs a UnitOfWork over pool.
func NewUnitOfWork(pool *pgxpool.Pool) *UnitOfWork {
	return &UnitOfWork{pool: pool}
}

// Execute runs fn inside one transaction, committing when fn returns nil
// and rolling back otherwise. A nested call (ctx already carrying a
// transaction) joins the outer one.
func (u *UnitOfWork) Execute(ctx context.Context, fn func(ctx context.Context) error) (err error) {
	if _, ok := pgtx.TxFrom(ctx); ok {
		return fn(ctx)
	}

	tx, err := u.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("analyticsstore: begin unit of work: %w", err)
	}
	defer func() {
		if p := recover(); p != nil {
			_ = tx.Rollback(ctx)
			panic(p)
		}
		if err != nil {
			if rbErr := tx.Rollback(ctx); rbErr != nil && !errors.Is(rbErr, pgx.ErrTxClosed) {
				err = errors.Join(err, fmt.Errorf("analyticsstore: rollback unit of work: %w", rbErr))
			}
		}
	}()

	if err = fn(pgtx.WithTx(ctx, tx)); err != nil {
		return err
	}
	if err = tx.Commit(ctx); err != nil {
		return fmt.Errorf("analyticsstore: commit unit of work: %w", err)
	}
	return nil
}
