package main_test

// Step definitions for features/handling_unit_standards.feature (ADR 0035).
// Like the wave-2 steps, completions are seeded through the real
// RecordTaskPerformance use case (the one the Kafka consumer calls) and every
// read assertion goes through the REST API or the recorded events.

import (
	"fmt"
	"net/http"
	"time"

	"github.com/cucumber/godog"

	"github.com/claudioed/labor-performance/internal/application/usecases"
	"github.com/claudioed/labor-performance/internal/domain/shared"
)

func (w *world) defineHandlingStandard(body map[string]any) error {
	return w.do(http.MethodPost, "/standards", body)
}

func (w *world) handlingStandardBody(expectedSeconds int, taskType, unit string) map[string]any {
	return map[string]any{"taskType": taskType, "handlingUnit": unit, "expectedSeconds": expectedSeconds}
}

func (w *world) defineUnitStandard(expectedSeconds int, taskType, unit string) error {
	return w.defineHandlingStandard(w.handlingStandardBody(expectedSeconds, taskType, unit))
}

func (w *world) defineUnitStandardWithTeamLift(expectedSeconds, thresholdG, liftSeconds int, taskType, unit string) error {
	body := w.handlingStandardBody(expectedSeconds, taskType, unit)
	body["teamLift"] = map[string]any{"thresholdG": thresholdG, "expectedSeconds": liftSeconds}
	return w.defineHandlingStandard(body)
}

func (w *world) requireCreated() error {
	if w.lastStatus != http.StatusCreated {
		return fmt.Errorf("define standard returned %d, want 201: %s", w.lastStatus, string(w.lastBody))
	}
	return nil
}

func (w *world) unitStandardAlreadyDefined(expectedSeconds int, taskType, unit string) error {
	if err := w.defineUnitStandard(expectedSeconds, taskType, unit); err != nil {
		return err
	}
	return w.requireCreated()
}

func (w *world) unitStandardWithTeamLiftAlreadyDefined(expectedSeconds, thresholdG, liftSeconds int, taskType, unit string) error {
	if err := w.defineUnitStandardWithTeamLift(expectedSeconds, thresholdG, liftSeconds, taskType, unit); err != nil {
		return err
	}
	return w.requireCreated()
}

func (w *world) getResolvedStandard(taskType, unit string) error {
	return w.do(http.MethodGet, "/standards/"+taskType+"?handlingUnit="+unit, nil)
}

func (w *world) standardReportsHandlingUnit(unit string) error {
	return w.expectString("handlingUnit", unit)
}

func (w *world) expectString(field, want string) error {
	got, err := w.stringField(field)
	if err != nil {
		return err
	}
	if got != want {
		return fmt.Errorf("%s is %q, want %q", field, got, want)
	}
	return nil
}

func (w *world) standardOmitsHandlingUnit() error {
	obj, err := w.decodeLast()
	if err != nil {
		return err
	}
	if _, present := obj["handlingUnit"]; present {
		return fmt.Errorf("response must omit handlingUnit: %s", string(w.lastBody))
	}
	return nil
}

func (w *world) standardReportsTeamLift(thresholdG, seconds, operators int) error {
	obj, err := w.decodeLast()
	if err != nil {
		return err
	}
	tl, ok := obj["teamLift"].(map[string]any)
	if !ok {
		return fmt.Errorf("response has no teamLift object: %s", string(w.lastBody))
	}
	for field, want := range map[string]int{"thresholdG": thresholdG, "expectedSeconds": seconds, "operators": operators} {
		if got, _ := tl[field].(float64); int(got) != want {
			return fmt.Errorf("teamLift.%s is %v, want %d", field, tl[field], want)
		}
	}
	return nil
}

// recordHandled seeds one completion that carries a handling unit (and
// optionally a case weight), timed after any standard defined in a Given.
func (s *wave2) recordHandled(associate, taskType, taskId, unit string, weightG *int64, actualSeconds int) error {
	at := activityBase.Add(time.Duration(actualSeconds) * time.Second)
	return s.record(usecases.RecordTaskPerformanceRequest{
		KafkaEventId:  "evt-" + taskId,
		TaskId:        taskId,
		AssociateId:   shared.AssociateId(associate),
		TaskType:      shared.TaskType(taskType),
		HandlingUnit:  shared.HandlingUnit(unit),
		WeightG:       weightG,
		ActualSeconds: int64(actualSeconds),
		CompletedAt:   at,
	})
}

func (s *wave2) completedHandled(associate, taskType, taskId, unit string, actualSeconds int) error {
	return s.recordHandled(associate, taskType, taskId, unit, nil, actualSeconds)
}

func (s *wave2) completedHandledWeighing(associate, taskType, taskId, unit string, weightG, actualSeconds int) error {
	g := int64(weightG)
	return s.recordHandled(associate, taskType, taskId, unit, &g, actualSeconds)
}

func (s *wave2) completedWithoutUnit(associate, taskType, taskId string, actualSeconds int) error {
	return s.recordHandled(associate, taskType, taskId, "", nil, actualSeconds)
}

func (s *wave2) recordedEventHandling(taskId, unit string, teamLift bool) error {
	ev, err := s.recordedEventFor(taskId)
	if err != nil {
		return err
	}
	if string(ev.HandlingUnit) != unit || ev.TeamLift != teamLift {
		return fmt.Errorf("event for %s carries unit %q team lift %v, want %q %v", taskId, ev.HandlingUnit, ev.TeamLift, unit, teamLift)
	}
	return nil
}

func (s *wave2) recordedEventNoHandling(taskId string) error {
	ev, err := s.recordedEventFor(taskId)
	if err != nil {
		return err
	}
	if ev.HandlingUnit != "" || ev.TeamLift {
		return fmt.Errorf("event for %s must carry no handling unit, got %q team lift %v", taskId, ev.HandlingUnit, ev.TeamLift)
	}
	return nil
}

func registerHandlingUnit(sc *godog.ScenarioContext, w *world, s *wave2) {
	const tt = `(PICK|PACK|SLAM)`
	const unit = `"([^"]*)"`
	const lift = `with a team lift above (\d+) grams of (\d+) seconds`

	sc.Step(`^a standard of (\d+) expected seconds is already defined for task type "([^"]*)" and handling unit `+unit+`$`, w.unitStandardAlreadyDefined)
	sc.Step(`^a standard of (\d+) expected seconds `+lift+` is already defined for task type "([^"]*)" and handling unit `+unit+`$`, w.unitStandardWithTeamLiftAlreadyDefined)
	sc.Step(`^a standard of (\d+) expected seconds is defined for task type "([^"]*)" and handling unit `+unit+`$`, w.defineUnitStandard)
	sc.Step(`^a standard of (\d+) expected seconds `+lift+` is defined for task type "([^"]*)" and handling unit `+unit+`$`, w.defineUnitStandardWithTeamLift)
	sc.Step(`^the standard for task type "([^"]*)" and handling unit `+unit+` is requested$`, w.getResolvedStandard)
	sc.Step(`^the standard response reports handling unit `+unit+`$`, w.standardReportsHandlingUnit)
	sc.Step(`^the standard response omits the handling unit$`, w.standardOmitsHandlingUnit)
	sc.Step(`^the standard response reports a team lift above (\d+) grams of (\d+) seconds with (\d+) operators$`, w.standardReportsTeamLift)

	sc.Step(`^associate "([^"]*)" completed a `+tt+` task "([^"]*)" for handling unit `+unit+` in (\d+) seconds$`, s.completedHandled)
	sc.Step(`^associate "([^"]*)" completed a `+tt+` task "([^"]*)" for handling unit `+unit+` weighing (\d+) grams in (\d+) seconds$`, s.completedHandledWeighing)
	sc.Step(`^associate "([^"]*)" completed a `+tt+` task "([^"]*)" in (\d+) seconds$`, s.completedWithoutUnit)
	sc.Step(`^the "TaskPerformanceRecorded" event for task "([^"]*)" carries handling unit `+unit+` and team lift (true|false)$`,
		func(taskId, u, lift string) error { return s.recordedEventHandling(taskId, u, lift == "true") })
	sc.Step(`^the "TaskPerformanceRecorded" event for task "([^"]*)" carries no handling unit$`, s.recordedEventNoHandling)
}
