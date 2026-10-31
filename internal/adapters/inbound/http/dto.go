// Package http is the inbound chi adapter: DTOs, handlers, routing, and
// domain-error-to-HTTP-status mapping. Domain structs never cross this
// boundary — every response below is a DTO owned by this package.
package http

// defineStandardRequest is the POST /standards request body.
type defineStandardRequest struct {
	TaskType        string `json:"taskType"`
	ExpectedSeconds int64  `json:"expectedSeconds"`
	// TravelComponentSeconds is an OPTIONAL declaration of how much of
	// ExpectedSeconds is attributable to travel between locations for
	// this TaskType. This service never computes it itself — it is
	// supplied by the caller, grounded in a real distance estimate
	// performed elsewhere (e.g. facility-layout's
	// estimate_travel_distance), since this service has no REST
	// dependency in either direction with any sibling context (ADR
	// 0015). Omitted entirely means "not broken out", the default.
	TravelComponentSeconds *int64 `json:"travelComponentSeconds,omitempty"`
	// HandlingUnit is OPTIONAL (ADR 0035): EACH, CASE or PALLET declares
	// the standard for that handling unit, which wins over the task-type-
	// only standard when a completed task carries that unit. Absent (or
	// empty) declares the task-type-only standard, exactly as before.
	HandlingUnit string `json:"handlingUnit,omitempty"`
	// TeamLift is OPTIONAL and only valid with handlingUnit CASE: the
	// declared heavy-case rule (ADR 0035).
	TeamLift *teamLiftRequest `json:"teamLift,omitempty"`
}

// teamLiftRequest is the declared heavy-case team-lift rule of a CASE
// standard: a case heavier than ThresholdG grams needs two operators and is
// scored against ExpectedSeconds.
type teamLiftRequest struct {
	ThresholdG      int64 `json:"thresholdG"`
	ExpectedSeconds int64 `json:"expectedSeconds"`
}

// teamLiftResponse echoes a declared team lift; Operators is the constant
// number of operators a team lift needs.
type teamLiftResponse struct {
	ThresholdG      int64 `json:"thresholdG"`
	ExpectedSeconds int64 `json:"expectedSeconds"`
	Operators       int   `json:"operators"`
}

// standardResponse is the response body for DefineStandard and
// GetStandard.
type standardResponse struct {
	TaskType        string `json:"taskType"`
	ExpectedSeconds int64  `json:"expectedSeconds"`
	// TravelComponentSeconds is omitted entirely (not defaulted to 0)
	// when this standard never declared one.
	TravelComponentSeconds *int64  `json:"travelComponentSeconds,omitempty"`
	EffectiveFrom          string  `json:"effectiveFrom"`
	EffectiveTo            *string `json:"effectiveTo,omitempty"`
	// HandlingUnit and TeamLift are omitted entirely for a task-type-only
	// standard, so its body is byte-for-byte what it was before ADR 0035.
	HandlingUnit string            `json:"handlingUnit,omitempty"`
	TeamLift     *teamLiftResponse `json:"teamLift,omitempty"`
}

// taskTypeBreakdownResponse is one TaskType's slice of a
// scorecardResponse.
type taskTypeBreakdownResponse struct {
	TaskCount         int      `json:"taskCount"`
	MeanEfficiencyPct *float64 `json:"meanEfficiencyPct"`
}

// scorecardResponse is the GET /associates/{associateId}/scorecard
// response body.
type scorecardResponse struct {
	AssociateId       string                               `json:"associateId"`
	TaskCount         int                                  `json:"taskCount"`
	MeanEfficiencyPct *float64                             `json:"meanEfficiencyPct"`
	ByTaskType        map[string]taskTypeBreakdownResponse `json:"byTaskType"`
	// Trend is one of IMPROVING, DECLINING, STABLE, or
	// INSUFFICIENT_DATA — see performance.ClassifyTrend. Always
	// present, never omitted: INSUFFICIENT_DATA is a real, meaningful
	// value, not an absence.
	Trend string `json:"trend"`
	// CoachingFlag is true iff this associate's most recent 3 scored
	// tasks were all below the coaching floor — see
	// performance.DetectCoachingFlag. Visibility only, never an
	// automated action.
	CoachingFlag bool `json:"coachingFlag"`
}

// taskTypePerformanceResponse is the GET /task-types/{taskType}/performance
// response body.
type taskTypePerformanceResponse struct {
	TaskType          string   `json:"taskType"`
	TaskCount         int      `json:"taskCount"`
	MeanEfficiencyPct *float64 `json:"meanEfficiencyPct"`
	// MeanActualSeconds is the real measured mean duration for this
	// TaskType, independent of whether an engineered standard exists.
	// See ports.TaskTypePerformance's doc comment for the full
	// distinction from MeanEfficiencyPct.
	MeanActualSeconds *float64 `json:"meanActualSeconds"`
}

// utilizationResponse is the response body for both
// GET /task-types/{taskType}/utilization and
// GET /associates/{associateId}/utilization. taskType/associateId are
// mutually exclusive — whichever endpoint served the request populates
// its own identifying field and omits the other.
type utilizationResponse struct {
	TaskType       string   `json:"taskType,omitempty"`
	AssociateId    string   `json:"associateId,omitempty"`
	Associates     int      `json:"associates"`
	WindowSeconds  int64    `json:"windowSeconds"`
	TaskSeconds    int64    `json:"taskSeconds"`
	IdleSeconds    int64    `json:"idleSeconds"`
	OpenGapSeconds int64    `json:"openGapSeconds"`
	UtilizationPct *float64 `json:"utilizationPct"`
}

// problemDetails is the RFC 7807 (Problem Details for HTTP APIs) response
// body used for every error response in this service — the same shape the
// other six services in this fleet emit.
type problemDetails struct {
	Type     string `json:"type"`
	Title    string `json:"title"`
	Status   int    `json:"status"`
	Detail   string `json:"detail"`
	Instance string `json:"instance,omitempty"`
}
