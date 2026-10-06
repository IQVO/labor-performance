package events

import (
	"time"

	"github.com/claudioed/labor-performance/internal/domain/shared"
)

// The log publisher's JSON payload shape lives here, in the adapter — the
// domain event structs carry no serialisation tags. Field names and order
// are the historical ones (pinned byte-for-byte by the goldens in
// testdata/): the camelCase eventName/occurredAt envelope first, then the
// event's own fields under their Go names.

// eventLogDTO is the payload of an event type this adapter has no
// dedicated DTO for: just the envelope.
type eventLogDTO struct {
	EventName  string    `json:"eventName"`
	OccurredAt time.Time `json:"occurredAt"`
}

type laborStandardDefinedLogDTO struct {
	EventName              string    `json:"eventName"`
	OccurredAt             time.Time `json:"occurredAt"`
	StandardId             string
	TaskType               string
	ExpectedSeconds        int64
	TravelComponentSeconds *int64
	EffectiveFrom          time.Time
}

type laborStandardRevisedLogDTO struct {
	EventName                 string    `json:"eventName"`
	OccurredAt                time.Time `json:"occurredAt"`
	StandardId                string
	TaskType                  string
	PreviousExpectedSeconds   int64
	NewExpectedSeconds        int64
	NewTravelComponentSeconds *int64
	EffectiveFrom             time.Time
}

type taskPerformanceRecordedLogDTO struct {
	EventName         string    `json:"eventName"`
	OccurredAt        time.Time `json:"occurredAt"`
	TaskId            string
	AssociateId       string
	TaskType          string
	ActualSeconds     int64
	EfficiencyPct     *float64
	IdleSecondsBefore *int64
	CompletedAt       time.Time
}

// logPayload maps a domain event to the DTO the log publisher marshals.
func logPayload(e shared.DomainEvent) any {
	switch ev := e.(type) {
	case shared.LaborStandardDefined:
		return laborStandardDefinedLogDTO{
			EventName:              ev.EventName(),
			OccurredAt:             ev.OccurredAt(),
			StandardId:             string(ev.StandardId),
			TaskType:               string(ev.TaskType),
			ExpectedSeconds:        ev.ExpectedSeconds,
			TravelComponentSeconds: ev.TravelComponentSeconds,
			EffectiveFrom:          ev.EffectiveFrom,
		}
	case shared.LaborStandardRevised:
		return laborStandardRevisedLogDTO{
			EventName:                 ev.EventName(),
			OccurredAt:                ev.OccurredAt(),
			StandardId:                string(ev.StandardId),
			TaskType:                  string(ev.TaskType),
			PreviousExpectedSeconds:   ev.PreviousExpectedSeconds,
			NewExpectedSeconds:        ev.NewExpectedSeconds,
			NewTravelComponentSeconds: ev.NewTravelComponentSeconds,
			EffectiveFrom:             ev.EffectiveFrom,
		}
	case shared.TaskPerformanceRecorded:
		return taskPerformanceRecordedLogDTO{
			EventName:         ev.EventName(),
			OccurredAt:        ev.OccurredAt(),
			TaskId:            ev.TaskId,
			AssociateId:       string(ev.AssociateId),
			TaskType:          string(ev.TaskType),
			ActualSeconds:     ev.ActualSeconds,
			EfficiencyPct:     ev.EfficiencyPct,
			IdleSecondsBefore: ev.IdleSecondsBefore,
			CompletedAt:       ev.CompletedAt,
		}
	default:
		return eventLogDTO{EventName: e.EventName(), OccurredAt: e.OccurredAt()}
	}
}
