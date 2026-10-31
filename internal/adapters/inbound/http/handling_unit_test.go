package http_test

import (
	"encoding/json"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/claudioed/labor-performance/internal/application/usecases"
	"github.com/claudioed/labor-performance/internal/domain/shared"
)

func decodeMap(t *testing.T, body []byte) map[string]any {
	t.Helper()
	var m map[string]any
	if err := json.Unmarshal(body, &m); err != nil {
		t.Fatalf("decode: %v (%s)", err, body)
	}
	return m
}

func TestPostStandards_CaseWithTeamLift_IsEchoed(t *testing.T) {
	e := newTestEnv(t, now)
	rec := e.do(t, http.MethodPost, "/standards", `{"taskType":"PICK","expectedSeconds":30,"handlingUnit":"CASE","teamLift":{"thresholdG":18000,"expectedSeconds":75}}`)
	if rec.Code != http.StatusCreated {
		t.Fatalf("status = %d (%s)", rec.Code, rec.Body.String())
	}
	m := decodeMap(t, rec.Body.Bytes())
	if m["handlingUnit"] != "CASE" {
		t.Fatalf("handlingUnit = %v", m["handlingUnit"])
	}
	tl, _ := m["teamLift"].(map[string]any)
	if tl["thresholdG"] != float64(18000) || tl["expectedSeconds"] != float64(75) || tl["operators"] != float64(2) {
		t.Fatalf("teamLift = %v", tl)
	}
}

func TestPostStandards_NoHandlingUnit_BodyIsUnchanged(t *testing.T) {
	e := newTestEnv(t, now)
	rec := e.do(t, http.MethodPost, "/standards", `{"taskType":"PICK","expectedSeconds":45}`)
	if rec.Code != http.StatusCreated {
		t.Fatalf("status = %d", rec.Code)
	}
	m := decodeMap(t, rec.Body.Bytes())
	for _, k := range []string{"handlingUnit", "teamLift"} {
		if _, present := m[k]; present {
			t.Fatalf("%s must be omitted for a task-type-only standard: %v", k, m)
		}
	}
	// An explicitly empty handlingUnit is the same as absent.
	rec = e.do(t, http.MethodPost, "/standards", `{"taskType":"PACK","expectedSeconds":45,"handlingUnit":""}`)
	if rec.Code != http.StatusCreated {
		t.Fatalf("empty handlingUnit status = %d (%s)", rec.Code, rec.Body.String())
	}
}

func TestPostStandards_HandlingUnitRejections(t *testing.T) {
	tests := []struct {
		name, body, slug string
		status           int
	}{
		{"unknown unit", `{"taskType":"PICK","expectedSeconds":30,"handlingUnit":"CARTON"}`, "unknown-handling-unit", http.StatusBadRequest},
		{"lowercase unit", `{"taskType":"PICK","expectedSeconds":30,"handlingUnit":"case"}`, "unknown-handling-unit", http.StatusBadRequest},
		{"team lift on EACH", `{"taskType":"PICK","expectedSeconds":30,"handlingUnit":"EACH","teamLift":{"thresholdG":1,"expectedSeconds":1}}`, "team-lift-requires-case-unit", http.StatusUnprocessableEntity},
		{"team lift on PALLET", `{"taskType":"PICK","expectedSeconds":30,"handlingUnit":"PALLET","teamLift":{"thresholdG":1,"expectedSeconds":1}}`, "team-lift-requires-case-unit", http.StatusUnprocessableEntity},
		{"team lift without unit", `{"taskType":"PICK","expectedSeconds":30,"teamLift":{"thresholdG":1,"expectedSeconds":1}}`, "team-lift-requires-case-unit", http.StatusUnprocessableEntity},
		{"zero threshold", `{"taskType":"PICK","expectedSeconds":30,"handlingUnit":"CASE","teamLift":{"thresholdG":0,"expectedSeconds":5}}`, "non-positive-team-lift-threshold", http.StatusUnprocessableEntity},
		{"negative threshold", `{"taskType":"PICK","expectedSeconds":30,"handlingUnit":"CASE","teamLift":{"thresholdG":-1,"expectedSeconds":5}}`, "non-positive-team-lift-threshold", http.StatusUnprocessableEntity},
		{"zero team lift seconds", `{"taskType":"PICK","expectedSeconds":30,"handlingUnit":"CASE","teamLift":{"thresholdG":5,"expectedSeconds":0}}`, "non-positive-team-lift-expected-seconds", http.StatusUnprocessableEntity},
		{"zero expected seconds still rejected", `{"taskType":"PICK","expectedSeconds":0,"handlingUnit":"CASE"}`, "non-positive-expected-seconds", http.StatusUnprocessableEntity},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			e := newTestEnv(t, now)
			p := assertProblem(t, e.do(t, http.MethodPost, "/standards", tc.body), tc.status)
			if !strings.HasSuffix(p.Type, "/"+tc.slug) {
				t.Fatalf("problem.type = %q, want suffix %q", p.Type, tc.slug)
			}
		})
	}
}

func TestGetStandard_HandlingUnitQuery_MostSpecificWins(t *testing.T) {
	e := newTestEnv(t, now)
	for _, body := range []string{
		`{"taskType":"PICK","expectedSeconds":45}`,
		`{"taskType":"PICK","expectedSeconds":30,"handlingUnit":"CASE"}`,
	} {
		if rec := e.do(t, http.MethodPost, "/standards", body); rec.Code != http.StatusCreated {
			t.Fatalf("setup %s: %d %s", body, rec.Code, rec.Body.String())
		}
	}
	get := func(path string) map[string]any {
		t.Helper()
		rec := e.do(t, http.MethodGet, path, "")
		if rec.Code != http.StatusOK {
			t.Fatalf("GET %s = %d (%s)", path, rec.Code, rec.Body.String())
		}
		return decodeMap(t, rec.Body.Bytes())
	}
	if m := get("/standards/PICK"); m["expectedSeconds"] != float64(45) || m["handlingUnit"] != nil {
		t.Fatalf("no query must stay the task-type-only standard: %v", m)
	}
	if m := get("/standards/PICK?handlingUnit=CASE"); m["expectedSeconds"] != float64(30) || m["handlingUnit"] != "CASE" {
		t.Fatalf("CASE must win: %v", m)
	}
	if m := get("/standards/PICK?handlingUnit=EACH"); m["expectedSeconds"] != float64(45) || m["handlingUnit"] != nil {
		t.Fatalf("EACH has no standard and falls back: %v", m)
	}
	if m := get("/standards/PICK?handlingUnit="); m["expectedSeconds"] != float64(45) {
		t.Fatalf("empty query value is no unit: %v", m)
	}
}

func TestGetStandard_HandlingUnitQueryErrors(t *testing.T) {
	e := newTestEnv(t, now)
	assertProblem(t, e.do(t, http.MethodGet, "/standards/PICK?handlingUnit=CARTON", ""), http.StatusBadRequest)
	// Nothing declared for either level: 404, not a fabricated standard.
	assertProblem(t, e.do(t, http.MethodGet, "/standards/PICK?handlingUnit=CASE", ""), http.StatusNotFound)
	// A CASE-only declaration does not answer the task-type-only query.
	if rec := e.do(t, http.MethodPost, "/standards", `{"taskType":"PICK","expectedSeconds":30,"handlingUnit":"CASE"}`); rec.Code != http.StatusCreated {
		t.Fatalf("setup: %d", rec.Code)
	}
	assertProblem(t, e.do(t, http.MethodGet, "/standards/PICK", ""), http.StatusNotFound)
}

func TestHandlingUnitStandard_ScoresACaseCompletion(t *testing.T) {
	e := newTestEnv(t, now)
	for _, body := range []string{
		`{"taskType":"PICK","expectedSeconds":45}`,
		`{"taskType":"PICK","expectedSeconds":30,"handlingUnit":"CASE","teamLift":{"thresholdG":18000,"expectedSeconds":75}}`,
	} {
		e.do(t, http.MethodPost, "/standards", body)
	}
	at := now.Add(time.Hour)
	e.recordViaBackdoor(t, usecases.RecordTaskPerformanceRequest{KafkaEventId: "e1", TaskId: "t1", AssociateId: "a1", TaskType: shared.Pick, HandlingUnit: shared.Case, ActualSeconds: 60, CompletedAt: at})
	rec := e.do(t, http.MethodGet, "/associates/a1/scorecard", "")
	m := decodeMap(t, rec.Body.Bytes())
	if m["meanEfficiencyPct"] != float64(50) { // 100*30/60
		t.Fatalf("scorecard = %v", m)
	}
}
