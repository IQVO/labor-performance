package usecases

import (
	"context"
	"errors"
	"log/slog"
	"time"

	"github.com/claudioed/labor-performance/internal/application/ports"
	"github.com/claudioed/labor-performance/internal/domain/idleness"
	"github.com/claudioed/labor-performance/internal/domain/performance"
	"github.com/claudioed/labor-performance/internal/domain/shared"
)

// defaultIdleGapCapSeconds is the fallback used when RecordTaskPerformance
// is constructed with IdleGapCapSeconds<=0 (the zero value, the common
// case for every existing caller/test that predates idleness). Matches
// the idleness ADR's documented default for IDLE_GAP_CAP_SECONDS: a gap
// spanning a shift boundary is capped rather than left to poison a
// running mean, without this service modeling shifts itself (WFM's
// domain, not this one's — see the ADR's "Cross-shift gaps" known
// limitation).
const defaultIdleGapCapSeconds = 3600

// RecordTaskPerformanceRequest is one already-completed task's facts, as
// carried on a fulfillment-execution TaskCompleted Kafka event.
type RecordTaskPerformanceRequest struct {
	KafkaEventId  string
	TaskId        string
	AssociateId   shared.AssociateId
	TaskType      shared.TaskType
	ActualSeconds int64
	CompletedAt   time.Time
}

// RecordTaskPerformance is the Kafka-consumer-driven use case: it is
// called from the inbound Kafka adapter, never from HTTP. It is idempotent
// on KafkaEventId — a redelivered/duplicate message is a no-op, not a
// double-count — and resolves whichever LaborStandard was active for
// TaskType AS OF CompletedAt (not "active right now"), so a possibly
// out-of-order or replayed message is scored against the standard that
// was genuinely in force when the task completed.
//
// Since the idleness feature, Execute additionally derives and records
// the associate's idle gap immediately preceding this task's claim (see
// internal/domain/idleness) on the SAME unit of work as the performance
// write, so a Kafka redelivery can never double-record either fact.
type RecordTaskPerformance struct {
	Performances ports.PerformanceRepo
	Standards    ports.StandardRepo
	Processed    ports.ProcessedEvents
	Events       ports.EventPublisher
	Clock        ports.Clock
	// UnitOfWork brackets MarkProcessed + Save + Publish atomically (ADR
	// 0010). Optional: nil means the calls run back to back, which is
	// the in-memory / log-publisher dev configuration.
	UnitOfWork ports.UnitOfWork
	// IdlePeriods persists the derived IdlePeriod aggregate. Optional:
	// nil means idle-gap derivation is skipped entirely (mirrors every
	// other optional dependency in this use case) — the wiring in
	// cmd/labor/main.go always supplies it, but tests that only care
	// about the performance-recording path can omit it.
	IdlePeriods ports.IdlePeriodRepo
	// IdleGapCapSeconds caps a derived idle gap's reported duration
	// (IDLE_GAP_CAP_SECONDS env, wired by the composition root).
	// <=0 falls back to defaultIdleGapCapSeconds — the common case for
	// every caller/test that predates idleness and never sets this
	// field.
	IdleGapCapSeconds int64
	// Logger receives a structured, non-fatal record when a derived
	// idle gap is skipped (idleness.ErrNegativeGap — Kafka delivery is
	// unordered, so this is a ROUTINE occurrence, not an exceptional
	// one). Optional: nil silences it, mirroring every other optional
	// dependency in this use case.
	Logger *slog.Logger
}

// Execute returns (nil, nil) when req.KafkaEventId was already processed
// — a benign no-op, not an error, so a consumer redelivery never appears
// as a failure.
//
// The idempotency marker, the TaskPerformance row, the derived IdlePeriod
// (when one exists) and the TaskPerformanceRecorded event are written in
// ONE atomic scope: if any of them fails, none survives, so a redelivered
// message after a partial failure is scored (the marker was rolled back
// too) rather than silently dropped as a "duplicate" of a row that never
// existed.
func (uc *RecordTaskPerformance) Execute(ctx context.Context, req RecordTaskPerformanceRequest) (*performance.TaskPerformance, error) {
	var p *performance.TaskPerformance
	err := atomically(ctx, uc.UnitOfWork, func(ctx context.Context) error {
		isNew, err := uc.Processed.MarkProcessed(ctx, req.KafkaEventId)
		if err != nil {
			return err
		}
		if !isNew {
			return nil
		}

		recorded, err := uc.record(ctx, req)
		if err != nil {
			return err
		}
		p = recorded
		return nil
	})
	if err != nil {
		return nil, err
	}
	return p, nil
}

// record performs the write phases for a first-time Kafka event id, in
// order: load the as-of standard, build the TaskPerformance aggregate,
// resolve the associate's prior completion, save the row, derive and save
// the idle gap, then publish TaskPerformanceRecorded. Every phase runs on
// the caller's unit of work, so a failure anywhere rolls them all back.
func (uc *RecordTaskPerformance) record(ctx context.Context, req RecordTaskPerformanceRequest) (*performance.TaskPerformance, error) {
	standardSecondsAtCompletion, err := uc.standardSecondsAtCompletion(ctx, req)
	if err != nil {
		return nil, err
	}

	recorded, err := performance.New(req.KafkaEventId, req.TaskId, req.AssociateId, req.TaskType, req.ActualSeconds, standardSecondsAtCompletion, req.CompletedAt)
	if err != nil {
		return nil, err
	}

	// Resolve the associate's PRIOR completion (the idle gap's start
	// boundary) BEFORE saving this row — Save makes `recorded` the
	// most recent row for this associate, so looking it up after
	// Save would see this task itself as its own "previous"
	// completion.
	previousCompletedAt, havePrevious, err := uc.previousCompletionFor(ctx, req.AssociateId)
	if err != nil {
		return nil, err
	}

	if err := uc.Performances.Save(ctx, recorded); err != nil {
		return nil, err
	}

	idleSecondsBefore, err := uc.recordIdleGap(ctx, req, previousCompletedAt, havePrevious)
	if err != nil {
		return nil, err
	}

	if err := uc.Events.Publish(ctx, shared.NewTaskPerformanceRecorded(uc.Clock.Now(), recorded.TaskId(), recorded.AssociateId(), recorded.TaskType(), recorded.ActualSeconds(), recorded.EfficiencyPct(), idleSecondsBefore, recorded.CompletedAt())); err != nil {
		return nil, err
	}
	return recorded, nil
}

// standardSecondsAtCompletion resolves whichever LaborStandard was active
// for req.TaskType AS OF req.CompletedAt (not "active right now"), so a
// possibly out-of-order or replayed message is scored against the standard
// that was genuinely in force when the task completed. Zero when the task
// type carries none — the task is recorded unscorable.
func (uc *RecordTaskPerformance) standardSecondsAtCompletion(ctx context.Context, req RecordTaskPerformanceRequest) (int64, error) {
	var standardSeconds int64
	if req.TaskType != "" {
		active, err := uc.Standards.FindActiveAsOf(ctx, req.TaskType, req.CompletedAt)
		if err != nil {
			return 0, err
		}
		if active != nil {
			standardSeconds = active.ExpectedSeconds()
		}
	}
	return standardSeconds, nil
}

// previousCompletionFor resolves the associate's most recent completion,
// if any, BEFORE this call's own row is saved — the idle gap's start
// boundary. havePrevious is false for a first-ever observation (the
// idleness ADR's "First-observation gap" limitation) or when idle-period
// recording is not wired (uc.IdlePeriods == nil) or associateId is empty
// (a robot station, "Robot stations" limitation) — in every such case no
// repo call is made at all.
func (uc *RecordTaskPerformance) previousCompletionFor(ctx context.Context, associateId shared.AssociateId) (completedAt time.Time, havePrevious bool, err error) {
	if uc.IdlePeriods == nil || associateId == "" {
		return time.Time{}, false, nil
	}
	previous, err := uc.Performances.RecentByAssociateID(ctx, associateId, 1)
	if err != nil {
		return time.Time{}, false, err
	}
	if len(previous) == 0 {
		return time.Time{}, false, nil
	}
	return previous[0].CompletedAt(), true, nil
}

// recordIdleGap derives the idle gap immediately preceding req's claim
// instant from the already-resolved previous completion, saves it, and
// returns the seconds to publish on the event — nil in every case the
// idleness ADR names as legitimate (no AssociateId, no prior completion,
// a negative/zero gap from out-of-order Kafka delivery, or idle-period
// recording not wired at all). Only an actual infrastructure error (a
// failing repo call) is propagated; every business-rule non-result is a
// nil, not an error, so it never fails the enclosing performance write.
func (uc *RecordTaskPerformance) recordIdleGap(ctx context.Context, req RecordTaskPerformanceRequest, previousCompletedAt time.Time, havePrevious bool) (*int64, error) {
	if uc.IdlePeriods == nil || req.AssociateId == "" || !havePrevious {
		return nil, nil
	}

	claimedAt := req.CompletedAt.Add(-time.Duration(req.ActualSeconds) * time.Second)
	capSeconds := uc.IdleGapCapSeconds
	if capSeconds <= 0 {
		capSeconds = defaultIdleGapCapSeconds
	}

	gap, err := idleness.New(req.AssociateId, req.TaskType, previousCompletedAt, claimedAt, capSeconds)
	if err != nil {
		if errors.Is(err, idleness.ErrNegativeGap) {
			// Kafka delivery is unordered — a "next" claim instant
			// landing at or before the previous completion is
			// ROUTINE, not exceptional. Skip and log, never fail
			// the performance write over it.
			if uc.Logger != nil {
				uc.Logger.WarnContext(ctx, "skipping idle gap: out-of-order Kafka delivery",
					"associate_id", string(req.AssociateId), "task_id", req.TaskId, "error", err)
			}
			return nil, nil
		}
		return nil, err
	}

	if err := uc.IdlePeriods.Save(ctx, gap); err != nil {
		return nil, err
	}
	seconds := gap.Seconds()
	return &seconds, nil
}
