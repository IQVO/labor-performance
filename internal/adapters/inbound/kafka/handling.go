package kafka

import (
	"encoding/json"

	"github.com/claudioed/labor-performance/internal/domain/shared"
)

// handlingUnitFrom leniently parses the optional `handling_unit` of a
// TaskCompleted payload (ADR 0035). Absent, null, not a string, or a value
// other than EACH/CASE/PALLET all degrade to "" (no handling unit), which
// scores the completion against the task-type-only standard.
func handlingUnitFrom(raw json.RawMessage) shared.HandlingUnit {
	if len(raw) == 0 {
		return ""
	}
	var s string
	if err := json.Unmarshal(raw, &s); err != nil {
		return ""
	}
	return shared.ParseHandlingUnitLenient(s)
}

// weightGFrom leniently parses the optional `weight_g` (integer grams of
// the handled case). Absent, null, non-integer or non-positive values are
// "unknown" (nil): a weight is never guessed and a non-positive weight is
// not a business fact.
func weightGFrom(raw json.RawMessage) *int64 {
	if len(raw) == 0 {
		return nil
	}
	var g int64
	if err := json.Unmarshal(raw, &g); err != nil || g <= 0 {
		return nil
	}
	return &g
}
