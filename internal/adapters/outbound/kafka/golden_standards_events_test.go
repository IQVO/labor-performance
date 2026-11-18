package kafka

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/claudioed/labor-performance/internal/adapters/kafka/cloudevents"
	"github.com/claudioed/labor-performance/internal/domain/shared"
)

// ADR 0038 goldens: LaborStandardDefined and LaborStandardRevised on the
// INTEGRATION topic. Same type, same `data` as the analytics copy pinned in
// golden_cloudevents_test.go / golden_handling_test.go (which are untouched),
// the `:events:` dataschema, partition key = task type, subject = standard id.

func TestGolden_Events_LaborStandardDefined(t *testing.T) {
	travel := int64(15)
	p := &IntegrationPublisher{NewID: fixedID}
	assertGolden(t, p, shared.NewLaborStandardDefined(at(9), "std-1", shared.Pick, 45, &travel, at(8)),
		cloudevents.TopicLaborPerformanceEvents, "PICK", `{
		"specversion":"1.0",
		"id":"0b6a3c1e-8f5d-4f8e-9c4b-2a7d1e5f6a90",
		"source":"/warehouse/labor-performance",
		"type":"com.warehouse.wes.labor-performance.standard.LaborStandardDefined",
		"subject":"std-1",
		"time":"2026-09-05T09:00:00Z",
		"datacontenttype":"application/json",
		"dataschema":"urn:warehouse:labor-performance:events:LaborStandardDefined:v1",
		"data":{"standard_id":"std-1","task_type":"PICK","expected_seconds":45,"effective_from":"2026-09-05T08:00:00Z","travel_component_seconds":15}
	}`)
}

func TestGolden_Events_LaborStandardRevised(t *testing.T) {
	p := &IntegrationPublisher{NewID: fixedID}
	assertGolden(t, p, shared.NewLaborStandardRevised(at(10), "std-2", shared.Pack, 45, 40, nil, at(10)),
		cloudevents.TopicLaborPerformanceEvents, "PACK", `{
		"specversion":"1.0",
		"id":"0b6a3c1e-8f5d-4f8e-9c4b-2a7d1e5f6a90",
		"source":"/warehouse/labor-performance",
		"type":"com.warehouse.wes.labor-performance.standard.LaborStandardRevised",
		"subject":"std-2",
		"time":"2026-09-05T10:00:00Z",
		"datacontenttype":"application/json",
		"dataschema":"urn:warehouse:labor-performance:events:LaborStandardRevised:v1",
		"data":{"standard_id":"std-2","task_type":"PACK","previous_expected_seconds":45,"expected_seconds":40,"effective_from":"2026-09-05T10:00:00Z"}
	}`)
}

func TestGolden_Events_LaborStandardDefined_CaseWithTeamLift(t *testing.T) {
	thr, secs := int64(18000), int64(75)
	p := &IntegrationPublisher{NewID: fixedID}
	assertGolden(t, p, shared.NewLaborStandardDefined(at(9), "std-9", shared.Pick, 30, nil, at(8)).WithHandling(shared.Case, &thr, &secs),
		cloudevents.TopicLaborPerformanceEvents, "PICK", `{
		"specversion":"1.0",
		"id":"0b6a3c1e-8f5d-4f8e-9c4b-2a7d1e5f6a90",
		"source":"/warehouse/labor-performance",
		"type":"com.warehouse.wes.labor-performance.standard.LaborStandardDefined",
		"subject":"std-9",
		"time":"2026-09-05T09:00:00Z",
		"datacontenttype":"application/json",
		"dataschema":"urn:warehouse:labor-performance:events:LaborStandardDefined:v1",
		"data":{"standard_id":"std-9","task_type":"PICK","expected_seconds":30,"effective_from":"2026-09-05T08:00:00Z","handling_unit":"CASE","team_lift_threshold_g":18000,"team_lift_expected_seconds":75}
	}`)
}

// wireParts splits one encoded CloudEvent into its raw data bytes and its
// context attributes (data removed).
func wireParts(t *testing.T, value []byte) (json.RawMessage, map[string]any) {
	t.Helper()
	var raw struct {
		Data json.RawMessage `json:"data"`
	}
	if err := json.Unmarshal(value, &raw); err != nil {
		t.Fatalf("value is not JSON: %v", err)
	}
	attrs := map[string]any{}
	if err := json.Unmarshal(value, &attrs); err != nil {
		t.Fatalf("value is not JSON: %v", err)
	}
	delete(attrs, "data")
	return raw.Data, attrs
}

// The integration copy of a standard event carries the analytics `data`
// BYTE FOR BYTE (one builder, never forked), and every context attribute
// except `dataschema` is the same; the topics differ, the key does not.
func TestStandardEvents_IntegrationDataIsTheAnalyticsDataByteForByte(t *testing.T) {
	travel, thr, secs := int64(15), int64(18000), int64(75)
	events := []shared.DomainEvent{
		shared.NewLaborStandardDefined(at(9), "std-1", shared.Pick, 45, &travel, at(8)),
		shared.NewLaborStandardDefined(at(9), "std-9", shared.Pick, 30, nil, at(8)).WithHandling(shared.Case, &thr, &secs),
		shared.NewLaborStandardRevised(at(10), "std-2", shared.Pack, 45, 40, nil, at(10)),
		shared.NewLaborStandardRevised(at(10), "std-10", shared.Pick, 95, 90, &travel, at(10)).WithHandling(shared.Pallet, nil, nil),
		shared.NewLaborStandardDefined(at(9), "std-11", shared.Pick, 40, nil, at(8)).WithSite("SITE-GRU-01"),
		shared.NewLaborStandardRevised(at(10), "std-12", shared.Pick, 30, 28, nil, at(10)).WithHandling(shared.Case, &thr, &secs).WithSite("SITE-GRU-01"),
	}
	analytics := &AnalyticsPublisher{NewID: fixedID}
	integration := &IntegrationPublisher{NewID: fixedID}
	for _, ev := range events {
		a, err := analytics.Encode(context.Background(), ev)
		if err != nil || len(a) != 1 {
			t.Fatalf("analytics Encode = %d msgs, %v", len(a), err)
		}
		i, err := integration.Encode(context.Background(), ev)
		if err != nil || len(i) != 1 {
			t.Fatalf("integration Encode = %d msgs, %v", len(i), err)
		}
		aData, aAttrs := wireParts(t, a[0].Value)
		iData, iAttrs := wireParts(t, i[0].Value)
		if string(aData) != string(iData) {
			t.Errorf("%s: integration data\n%s\nis not the analytics data\n%s", ev.EventName(), iData, aData)
		}
		if aAttrs["dataschema"] != cloudevents.DataSchema(cloudevents.StreamAnalytics, ev.EventName(), 1) ||
			iAttrs["dataschema"] != cloudevents.DataSchema(cloudevents.StreamEvents, ev.EventName(), 1) {
			t.Errorf("%s: dataschemas %v / %v", ev.EventName(), aAttrs["dataschema"], iAttrs["dataschema"])
		}
		delete(aAttrs, "dataschema")
		delete(iAttrs, "dataschema")
		ab, _ := json.Marshal(aAttrs)
		ib, _ := json.Marshal(iAttrs)
		if string(ab) != string(ib) {
			t.Errorf("%s: context attributes differ beyond dataschema\n analytics: %s\nintegration: %s", ev.EventName(), ab, ib)
		}
		if string(a[0].Key) != string(i[0].Key) || a[0].EventType != i[0].EventType {
			t.Errorf("%s: key/type %q %q vs %q %q", ev.EventName(), a[0].Key, a[0].EventType, i[0].Key, i[0].EventType)
		}
		if a[0].Topic != cloudevents.TopicLaborPerformanceAnalytics || i[0].Topic != cloudevents.TopicLaborPerformanceEvents {
			t.Errorf("%s: topics %q / %q", ev.EventName(), a[0].Topic, i[0].Topic)
		}
	}
}
