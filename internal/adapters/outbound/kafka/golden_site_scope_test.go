package kafka

import (
	"context"
	"strings"
	"testing"

	"github.com/claudioed/labor-performance/internal/adapters/kafka/cloudevents"
	"github.com/claudioed/labor-performance/internal/domain/shared"
)

// ADR 0038 decision 2 goldens: a site-scoped standard adds `site_code` to
// the data of BOTH copies (integration and analytics), right after the
// fields it had before; a site-less standard omits it entirely, so every
// pre-ADR-0038 golden (golden_cloudevents_test.go, golden_handling_test.go,
// golden_standards_events_test.go) stays byte-identical.

func TestGolden_Events_LaborStandardDefined_SiteScoped(t *testing.T) {
	p := &IntegrationPublisher{NewID: fixedID}
	assertGolden(t, p, shared.NewLaborStandardDefined(at(9), "std-11", shared.Pick, 40, nil, at(8)).WithSite("SITE-GRU-01"),
		cloudevents.TopicLaborPerformanceEvents, "PICK", `{
		"specversion":"1.0",
		"id":"0b6a3c1e-8f5d-4f8e-9c4b-2a7d1e5f6a90",
		"source":"/warehouse/labor-performance",
		"type":"com.warehouse.wes.labor-performance.standard.LaborStandardDefined",
		"subject":"std-11",
		"time":"2026-09-05T09:00:00Z",
		"datacontenttype":"application/json",
		"dataschema":"urn:warehouse:labor-performance:events:LaborStandardDefined:v1",
		"data":{"standard_id":"std-11","task_type":"PICK","expected_seconds":40,"effective_from":"2026-09-05T08:00:00Z","site_code":"SITE-GRU-01"}
	}`)
}

func TestGolden_Events_LaborStandardRevised_SiteScopedCase(t *testing.T) {
	thr, secs := int64(18000), int64(75)
	p := &IntegrationPublisher{NewID: fixedID}
	assertGolden(t, p, shared.NewLaborStandardRevised(at(10), "std-12", shared.Pick, 30, 28, nil, at(10)).
		WithHandling(shared.Case, &thr, &secs).WithSite("SITE-GRU-01"),
		cloudevents.TopicLaborPerformanceEvents, "PICK", `{
		"specversion":"1.0",
		"id":"0b6a3c1e-8f5d-4f8e-9c4b-2a7d1e5f6a90",
		"source":"/warehouse/labor-performance",
		"type":"com.warehouse.wes.labor-performance.standard.LaborStandardRevised",
		"subject":"std-12",
		"time":"2026-09-05T10:00:00Z",
		"datacontenttype":"application/json",
		"dataschema":"urn:warehouse:labor-performance:events:LaborStandardRevised:v1",
		"data":{"standard_id":"std-12","task_type":"PICK","previous_expected_seconds":30,"expected_seconds":28,"effective_from":"2026-09-05T10:00:00Z","handling_unit":"CASE","team_lift_threshold_g":18000,"team_lift_expected_seconds":75,"site_code":"SITE-GRU-01"}
	}`)
}

func TestGolden_Analytics_LaborStandardDefined_SiteScoped(t *testing.T) {
	p := &AnalyticsPublisher{NewID: fixedID}
	assertGolden(t, p, shared.NewLaborStandardDefined(at(9), "std-11", shared.Pick, 40, nil, at(8)).WithSite("SITE-GRU-01"),
		cloudevents.TopicLaborPerformanceAnalytics, "PICK", `{
		"specversion":"1.0",
		"id":"0b6a3c1e-8f5d-4f8e-9c4b-2a7d1e5f6a90",
		"source":"/warehouse/labor-performance",
		"type":"com.warehouse.wes.labor-performance.standard.LaborStandardDefined",
		"subject":"std-11",
		"time":"2026-09-05T09:00:00Z",
		"datacontenttype":"application/json",
		"dataschema":"urn:warehouse:labor-performance:analytics:LaborStandardDefined:v1",
		"data":{"standard_id":"std-11","task_type":"PICK","expected_seconds":40,"effective_from":"2026-09-05T08:00:00Z","site_code":"SITE-GRU-01"}
	}`)
}

func TestGolden_Analytics_LaborStandardRevised_SiteScoped(t *testing.T) {
	p := &AnalyticsPublisher{NewID: fixedID}
	assertGolden(t, p, shared.NewLaborStandardRevised(at(10), "std-13", shared.Slam, 20, 18, nil, at(10)).WithSite("SITE-POA-02"),
		cloudevents.TopicLaborPerformanceAnalytics, "SLAM", `{
		"specversion":"1.0",
		"id":"0b6a3c1e-8f5d-4f8e-9c4b-2a7d1e5f6a90",
		"source":"/warehouse/labor-performance",
		"type":"com.warehouse.wes.labor-performance.standard.LaborStandardRevised",
		"subject":"std-13",
		"time":"2026-09-05T10:00:00Z",
		"datacontenttype":"application/json",
		"dataschema":"urn:warehouse:labor-performance:analytics:LaborStandardRevised:v1",
		"data":{"standard_id":"std-13","task_type":"SLAM","previous_expected_seconds":20,"expected_seconds":18,"effective_from":"2026-09-05T10:00:00Z","site_code":"SITE-POA-02"}
	}`)
}

// TestStandardEvents_SiteLessDataHasNoSiteKey: an explicit empty site is
// the site-less standard and serialises exactly as before (no key, never
// null or "").
func TestStandardEvents_SiteLessDataHasNoSiteKey(t *testing.T) {
	events := []shared.DomainEvent{
		shared.NewLaborStandardDefined(at(9), "std-1", shared.Pick, 45, nil, at(8)).WithSite(""),
		shared.NewLaborStandardRevised(at(10), "std-2", shared.Pack, 45, 40, nil, at(10)).WithSite(""),
	}
	encoders := map[string]Encoder{
		"analytics":   &AnalyticsPublisher{NewID: fixedID},
		"integration": &IntegrationPublisher{NewID: fixedID},
	}
	for _, ev := range events {
		for name, enc := range encoders {
			msgs, err := enc.Encode(context.Background(), ev)
			if err != nil || len(msgs) != 1 {
				t.Fatalf("%s %s: Encode = %d msgs, %v", name, ev.EventName(), len(msgs), err)
			}
			if strings.Contains(string(msgs[0].Value), "site_code") {
				t.Fatalf("%s %s: site_code must be omitted for a site-less standard: %s", name, ev.EventName(), msgs[0].Value)
			}
		}
	}
}
