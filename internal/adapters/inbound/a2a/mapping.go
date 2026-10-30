package a2a

import (
	"fmt"

	"github.com/claudioed/labor-performance/internal/application/ports"
	"github.com/claudioed/labor-performance/internal/application/usecases"
	"github.com/claudioed/labor-performance/internal/domain/standard"
)

// problemTypeBase namespaces this context's problem types, mirroring the
// RFC 7807 practice of the HTTP adapter (which uses the same host).
const problemTypeBase = "https://errors.labor-performance.warehouse-systems.dev"

// problem is the RFC 7807-shaped rejection an agent client receives instead
// of a result: domain rejections and request-shape failures are business
// answers, not transport errors, so they travel in the message envelope.
type problem struct {
	Type   string `json:"type"`
	Title  string `json:"title"`
	Detail string `json:"detail"`
	Status int    `json:"status"`
}

// summary renders the one-line form used for the reply's text part.
func (p *problem) summary() string {
	return fmt.Sprintf("%s (%d): %s", p.Title, p.Status, p.Detail)
}

func problemMalformedRequest(detail string) *problem {
	return &problem{Type: problemTypeBase + "/malformed-skill-request", Title: "Malformed skill request", Detail: detail, Status: 400}
}

func problemUnknownSkill(skill string) *problem {
	return &problem{Type: problemTypeBase + "/unknown-skill", Title: "Unknown skill", Detail: fmt.Sprintf("skill %q is not offered by this agent; the Agent Card lists the supported skills", skill), Status: 400}
}

func problemInvalidArgument(detail string) *problem {
	return &problem{Type: problemTypeBase + "/invalid-argument", Title: "Invalid skill argument", Detail: detail, Status: 400}
}

// envelope is the structured payload of every skill answer: exactly one of
// Data (a skill DTO) or Problem (why there is no result), plus the skill id
// the answer belongs to.
type envelope struct {
	Skill   string   `json:"skill"`
	Data    skillDTO `json:"data,omitempty"`
	Problem *problem `json:"problem,omitempty"`
}

// skillDTO is the union of the skill result DTOs. A2A parts are
// machine-readable JSON; each skill fills its own field, keeping the
// envelope flat and versioned as one contract.
type skillDTO struct {
	AssociateScorecard *scorecardDTO  `json:"associateScorecard,omitempty"`
	TaskTypeStatus     *taskTypeState `json:"taskTypeStatus,omitempty"`
	summary            string
}

// taskTypeBreakdownDTO is one TaskType's slice of a scorecardDTO.
type taskTypeBreakdownDTO struct {
	TaskCount         int      `json:"taskCount"`
	MeanEfficiencyPct *float64 `json:"meanEfficiencyPct"`
}

// scorecardDTO is the associate_scorecard result. Nullable figures stay null
// (never 0) when nothing was scorable, as on the REST and MCP surfaces.
type scorecardDTO struct {
	AssociateId       string                          `json:"associateId"`
	TaskCount         int                             `json:"taskCount"`
	MeanEfficiencyPct *float64                        `json:"meanEfficiencyPct"`
	ByTaskType        map[string]taskTypeBreakdownDTO `json:"byTaskType"`
	// Trend is one of IMPROVING, DECLINING, STABLE or INSUFFICIENT_DATA.
	Trend string `json:"trend"`
	// CoachingFlag is a visibility signal for a human, never an automated
	// trigger.
	CoachingFlag bool `json:"coachingFlag"`
}

func toScorecardDTO(sc ports.Scorecard) scorecardDTO {
	byTaskType := make(map[string]taskTypeBreakdownDTO, len(sc.ByTaskType))
	for tt, b := range sc.ByTaskType {
		byTaskType[string(tt)] = taskTypeBreakdownDTO{TaskCount: b.TaskCount, MeanEfficiencyPct: b.MeanEfficiencyPct}
	}
	return scorecardDTO{
		AssociateId:       string(sc.AssociateId),
		TaskCount:         sc.TaskCount,
		MeanEfficiencyPct: sc.MeanEfficiencyPct,
		ByTaskType:        byTaskType,
		Trend:             string(sc.Trend),
		CoachingFlag:      sc.CoachingFlag,
	}
}

func (s scorecardDTO) oneLine() string {
	return fmt.Sprintf("%s: %d tasks, mean efficiency %s, trend %s, coaching flag %t",
		s.AssociateId, s.TaskCount, pct(s.MeanEfficiencyPct), s.Trend, s.CoachingFlag)
}

// taskTypeState is the composed task_type_status result: the fleet-wide
// performance read model, the active standard (nil when none) and the
// utilization over the window, side by side.
type taskTypeState struct {
	TaskType    string         `json:"taskType"`
	Performance performanceDTO `json:"performance"`
	Standard    *standardDTO   `json:"standard"`
	Utilization utilizationDTO `json:"utilization"`
}

type performanceDTO struct {
	TaskCount         int      `json:"taskCount"`
	MeanEfficiencyPct *float64 `json:"meanEfficiencyPct"`
	// MeanActualSeconds is the real measured mean duration, independent of
	// whether an engineered standard exists.
	MeanActualSeconds *float64 `json:"meanActualSeconds"`
}

type utilizationDTO struct {
	Associates     int   `json:"associates"`
	WindowSeconds  int64 `json:"windowSeconds"`
	TaskSeconds    int64 `json:"taskSeconds"`
	IdleSeconds    int64 `json:"idleSeconds"`
	OpenGapSeconds int64 `json:"openGapSeconds"`
	// UtilizationPct is null when there was nothing to compute a share of.
	UtilizationPct *float64 `json:"utilizationPct"`
}

type standardDTO struct {
	ExpectedSeconds int64 `json:"expectedSeconds"`
	// TravelComponentSeconds is omitted (not defaulted to 0) when this
	// standard never declared one.
	TravelComponentSeconds *int64  `json:"travelComponentSeconds,omitempty"`
	EffectiveFrom          string  `json:"effectiveFrom"`
	EffectiveTo            *string `json:"effectiveTo,omitempty"`
}

const timeLayout = "2006-01-02T15:04:05Z07:00"

func toStandardDTO(s *standard.LaborStandard) *standardDTO {
	if s == nil {
		return nil
	}
	dto := &standardDTO{
		ExpectedSeconds:        s.ExpectedSeconds(),
		TravelComponentSeconds: s.TravelComponentSeconds(),
		EffectiveFrom:          s.EffectiveFrom().UTC().Format(timeLayout),
	}
	if to := s.EffectiveTo(); to != nil {
		formatted := to.UTC().Format(timeLayout)
		dto.EffectiveTo = &formatted
	}
	return dto
}

func toTaskTypeState(p ports.TaskTypePerformance, s *standard.LaborStandard, u usecases.UtilizationResult) taskTypeState {
	return taskTypeState{
		TaskType: string(p.TaskType),
		Performance: performanceDTO{
			TaskCount:         p.TaskCount,
			MeanEfficiencyPct: p.MeanEfficiencyPct,
			MeanActualSeconds: p.MeanActualSeconds,
		},
		Standard: toStandardDTO(s),
		Utilization: utilizationDTO{
			Associates:     u.Associates,
			WindowSeconds:  u.WindowSeconds,
			TaskSeconds:    u.TaskSeconds,
			IdleSeconds:    u.IdleSeconds,
			OpenGapSeconds: u.OpenGapSeconds,
			UtilizationPct: u.UtilizationPct,
		},
	}
}

func (t taskTypeState) oneLine() string {
	std := "no active standard"
	if t.Standard != nil {
		std = fmt.Sprintf("standard %ds", t.Standard.ExpectedSeconds)
	}
	return fmt.Sprintf("%s: %d tasks, mean efficiency %s, %s, utilization %s over %ds",
		t.TaskType, t.Performance.TaskCount, pct(t.Performance.MeanEfficiencyPct), std,
		pct(t.Utilization.UtilizationPct), t.Utilization.WindowSeconds)
}

// pct renders a nullable percentage for the text summary: null is "n/a",
// never "0%".
func pct(v *float64) string {
	if v == nil {
		return "n/a"
	}
	return fmt.Sprintf("%.1f%%", *v)
}
