package kafka

import "github.com/claudioed/labor-performance/internal/domain/shared"

// addHandling adds the ADR 0035 fields to a TaskPerformanceRecorded payload,
// ONLY when the completion carried a handling unit. A completion without
// one keeps the exact pre-ADR-0035 payload (additive, never null-filled), so
// a strict golden of the old shape and every unknown-field-tolerant
// consumer are unaffected.
func addHandling(data map[string]any, ev shared.TaskPerformanceRecorded) {
	if ev.HandlingUnit == "" {
		return
	}
	data["handling_unit"] = string(ev.HandlingUnit)
	data["team_lift"] = ev.TeamLift
}

// addStandardHandling adds the ADR 0035 fields to a LaborStandardDefined /
// LaborStandardRevised payload: handling_unit when the standard is specific
// to one, and the team-lift declaration when it declared one. Both are
// omitted entirely (not null) otherwise.
func addStandardHandling(fields map[string]any, unit shared.HandlingUnit, thresholdG, teamLiftSeconds *int64) {
	if unit != "" {
		fields["handling_unit"] = string(unit)
	}
	if thresholdG != nil && teamLiftSeconds != nil {
		fields["team_lift_threshold_g"] = *thresholdG
		fields["team_lift_expected_seconds"] = *teamLiftSeconds
	}
}
