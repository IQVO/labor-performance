package kafka

import (
	"testing"

	"github.com/claudioed/labor-performance/internal/adapters/kafka/cloudevents"
	"github.com/claudioed/labor-performance/internal/domain/shared"
)

// ADR 0035 goldens: the handling-unit fields are additive. They appear only
// when a handling unit is involved; the pre-ADR payloads are pinned by the
// existing goldens in golden_cloudevents_test.go, which are untouched.

func TestGolden_Analytics_LaborStandardDefined_CaseWithTeamLift(t *testing.T) {
	thr, secs := int64(18000), int64(75)
	p := &AnalyticsPublisher{NewID: fixedID}
	assertGolden(t, p, shared.NewLaborStandardDefined(at(9), "std-9", shared.Pick, 30, nil, at(8)).WithHandling(shared.Case, &thr, &secs),
		cloudevents.TopicLaborPerformanceAnalytics, "PICK", `{
		"specversion":"1.0",
		"id":"0b6a3c1e-8f5d-4f8e-9c4b-2a7d1e5f6a90",
		"source":"/warehouse/labor-performance",
		"type":"com.warehouse.wes.labor-performance.standard.LaborStandardDefined",
		"subject":"std-9",
		"time":"2026-09-05T09:00:00Z",
		"datacontenttype":"application/json",
		"dataschema":"urn:warehouse:labor-performance:analytics:LaborStandardDefined:v1",
		"data":{"standard_id":"std-9","task_type":"PICK","expected_seconds":30,"effective_from":"2026-09-05T08:00:00Z","handling_unit":"CASE","team_lift_threshold_g":18000,"team_lift_expected_seconds":75}
	}`)
}

func TestGolden_Analytics_LaborStandardRevised_PalletOmitsTeamLift(t *testing.T) {
	p := &AnalyticsPublisher{NewID: fixedID}
	assertGolden(t, p, shared.NewLaborStandardRevised(at(10), "std-10", shared.Pick, 95, 90, nil, at(10)).WithHandling(shared.Pallet, nil, nil),
		cloudevents.TopicLaborPerformanceAnalytics, "PICK", `{
		"specversion":"1.0",
		"id":"0b6a3c1e-8f5d-4f8e-9c4b-2a7d1e5f6a90",
		"source":"/warehouse/labor-performance",
		"type":"com.warehouse.wes.labor-performance.standard.LaborStandardRevised",
		"subject":"std-10",
		"time":"2026-09-05T10:00:00Z",
		"datacontenttype":"application/json",
		"dataschema":"urn:warehouse:labor-performance:analytics:LaborStandardRevised:v1",
		"data":{"standard_id":"std-10","task_type":"PICK","previous_expected_seconds":95,"expected_seconds":90,"effective_from":"2026-09-05T10:00:00Z","handling_unit":"PALLET"}
	}`)
}

func TestGolden_Analytics_LaborStandardRevised_TeamLiftRequiresBothColumns(t *testing.T) {
	thr := int64(18000)
	p := &AnalyticsPublisher{NewID: fixedID}
	// A half-declared team lift is not a declaration: neither field is emitted.
	assertGolden(t, p, shared.NewLaborStandardRevised(at(10), "std-11", shared.Pick, 30, 29, nil, at(10)).WithHandling(shared.Case, &thr, nil),
		cloudevents.TopicLaborPerformanceAnalytics, "PICK", `{
		"specversion":"1.0",
		"id":"0b6a3c1e-8f5d-4f8e-9c4b-2a7d1e5f6a90",
		"source":"/warehouse/labor-performance",
		"type":"com.warehouse.wes.labor-performance.standard.LaborStandardRevised",
		"subject":"std-11",
		"time":"2026-09-05T10:00:00Z",
		"datacontenttype":"application/json",
		"dataschema":"urn:warehouse:labor-performance:analytics:LaborStandardRevised:v1",
		"data":{"standard_id":"std-11","task_type":"PICK","previous_expected_seconds":30,"expected_seconds":29,"effective_from":"2026-09-05T10:00:00Z","handling_unit":"CASE"}
	}`)
}

func TestGolden_Analytics_TaskPerformanceRecorded_CaseTeamLift(t *testing.T) {
	pct := 40.0
	p := &AnalyticsPublisher{NewID: fixedID}
	assertGolden(t, p, shared.NewTaskPerformanceRecorded(at(11), "task-1", "assoc-1", shared.Pick, 75, &pct, nil, at(9)).WithHandling(shared.Case, true),
		cloudevents.TopicLaborPerformanceAnalytics, "PICK", `{
		"specversion":"1.0",
		"id":"0b6a3c1e-8f5d-4f8e-9c4b-2a7d1e5f6a90",
		"source":"/warehouse/labor-performance",
		"type":"com.warehouse.wes.labor-performance.performance.TaskPerformanceRecorded",
		"subject":"assoc-1",
		"time":"2026-09-05T11:00:00Z",
		"datacontenttype":"application/json",
		"dataschema":"urn:warehouse:labor-performance:analytics:TaskPerformanceRecorded:v1",
		"data":{"task_id":"task-1","associate_id":"assoc-1","task_type":"PICK","efficiency_pct":40,"actual_seconds":75,"idle_seconds_before":null,"completed_at":"2026-09-05T09:00:00Z","handling_unit":"CASE","team_lift":true}
	}`)
}

func TestGolden_Events_TaskPerformanceRecorded_CaseNotTeamLift(t *testing.T) {
	pct := 100.0
	p := &IntegrationPublisher{NewID: fixedID}
	assertGolden(t, p, shared.NewTaskPerformanceRecorded(at(11), "task-1", "assoc-1", shared.Pick, 30, &pct, nil, at(9)).WithHandling(shared.Case, false),
		cloudevents.TopicLaborPerformanceEvents, "assoc-1", `{
		"specversion":"1.0",
		"id":"0b6a3c1e-8f5d-4f8e-9c4b-2a7d1e5f6a90",
		"source":"/warehouse/labor-performance",
		"type":"com.warehouse.wes.labor-performance.performance.TaskPerformanceRecorded",
		"subject":"assoc-1",
		"time":"2026-09-05T11:00:00Z",
		"datacontenttype":"application/json",
		"dataschema":"urn:warehouse:labor-performance:events:TaskPerformanceRecorded:v1",
		"data":{"task_id":"task-1","associate_id":"assoc-1","task_type":"PICK","efficiency_pct":100,"actual_seconds":30,"idle_seconds_before":null,"completed_at":"2026-09-05T09:00:00Z","handling_unit":"CASE","team_lift":false}
	}`)
}
