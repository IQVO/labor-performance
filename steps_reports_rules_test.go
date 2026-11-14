package main_test

// Report steps: the real AnalyticsConsumer projects the real encoded analytics
// messages into the in-memory report store, and the real reports router
// (cmd/labor-reports) serves it.

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"time"

	"github.com/cucumber/godog"

	inboundhttp "github.com/claudioed/labor-performance/internal/adapters/inbound/http"
	inboundkafka "github.com/claudioed/labor-performance/internal/adapters/inbound/kafka"
	"github.com/claudioed/labor-performance/internal/adapters/kafka/cloudevents"
	"github.com/claudioed/labor-performance/internal/adapters/outbound/analyticsstore"
	"github.com/claudioed/labor-performance/internal/analytics/report"
)

// switchStore is the report store the reader sees: the real in-memory store,
// until a scenario takes it down.
type switchStore struct {
	inner *analyticsstore.MemoryStore
	down  atomic.Bool
}

var errStoreDown = fmt.Errorf("analytical database unreachable")

func (s *switchStore) Query(ctx context.Context, q report.ReportQuery) (report.LaborPerformanceReport, error) {
	if s.down.Load() {
		return report.LaborPerformanceReport{}, errStoreDown
	}
	return s.inner.Query(ctx, q)
}

func (s *switchStore) FreshnessLag(ctx context.Context) (time.Duration, error) {
	if s.down.Load() {
		return 0, errStoreDown
	}
	return s.inner.FreshnessLag(ctx)
}

type reportsState struct {
	store    *analyticsstore.MemoryStore
	gate     *switchStore
	consumer *inboundkafka.AnalyticsConsumer
	server   *httptest.Server
	// projected counts the analytics messages already handed to the projector.
	projected int
}

// ensureReports builds the projector and the reports reader on first use.
func (r *rules) ensureReports() *reportsState {
	if r.reports != nil {
		return r.reports
	}
	store := analyticsstore.NewMemoryStore()
	store.Now = r.w.h.clock.Now
	gate := &switchStore{inner: store}
	quiet := slog.New(slog.NewTextHandler(io.Discard, nil))
	r.reports = &reportsState{
		store: store,
		gate:  gate,
		consumer: &inboundkafka.AnalyticsConsumer{
			Projection: store, Processed: store, Logger: quiet,
		},
		server: httptest.NewServer(inboundhttp.NewReportsRouter(&inboundhttp.ReportsHandlers{Store: gate}, quiet, "")),
	}
	return r.reports
}

func (r *rules) closeReports() {
	if r.reports != nil {
		r.reports.server.Close()
		r.reports = nil
	}
}

func (r *rules) projectPublished() error {
	rs := r.ensureReports()
	if err := r.encodeNew(); err != nil {
		return err
	}
	seen := 0
	for _, m := range r.wire.msgs {
		if m.stream != "analytics" {
			continue
		}
		seen++
		if seen <= rs.projected {
			continue
		}
		if err := rs.consumer.HandleMessage(context.Background(), m.enc.Value); err != nil {
			return fmt.Errorf("project %s: %w", m.name, err)
		}
	}
	rs.projected = seen
	return nil
}

func (r *rules) projectAgain(name string) error {
	rs := r.ensureReports()
	m, err := r.lastMessage("analytics", name)
	if err != nil {
		return err
	}
	return rs.consumer.HandleMessage(context.Background(), m.enc.Value)
}

// skipped feeds one raw message to the projector and requires that it is
// accepted without error (so the consume loop commits past it) -- the report
// itself is asserted by the steps that follow.
func (r *rules) skipped(raw []byte) error {
	if err := r.ensureReports().consumer.HandleMessage(context.Background(), raw); err != nil {
		return fmt.Errorf("the projector must skip this message, got error: %w", err)
	}
	return nil
}

func (r *rules) legacyFlatEnvelope() error {
	return r.skipped([]byte(`{"event":"TaskPerformanceRecorded","task_type":"PICK","actual_seconds":60}`))
}

func (r *rules) unrelatedType(name string) error {
	raw, err := cloudevents.New(cloudevents.Spec{
		ID: "ce-unrelated", Entity: cloudevents.EntityPerformance, EventName: name, Subject: "A-1",
		Time: r.w.h.clock.Now(), Stream: cloudevents.StreamAnalytics, Version: 1,
		Data: map[string]any{"task_type": "PICK", "actual_seconds": 60},
	})
	if err != nil {
		return err
	}
	return r.skipped(raw)
}

func (r *rules) undecodableData() error {
	raw, err := cloudevents.New(cloudevents.Spec{
		ID: "ce-bad-data", Entity: cloudevents.EntityPerformance, EventName: cloudevents.EventTaskPerformanceRecorded, Subject: "A-1",
		Time: r.w.h.clock.Now(), Stream: cloudevents.StreamAnalytics, Version: 1,
		Data: map[string]any{"task_type": "PICK", "actual_seconds": "fifty"},
	})
	if err != nil {
		return err
	}
	return r.skipped(raw)
}

func (r *rules) storeDown() error {
	r.ensureReports().gate.down.Store(true)
	return nil
}

func (r *rules) reportRequested(query string) error {
	rs := r.ensureReports()
	return r.w.doURL(http.MethodGet, rs.server.URL+"/reports/performance?"+query, nil)
}

func (r *rules) freshnessRequested() error {
	rs := r.ensureReports()
	return r.w.doURL(http.MethodGet, rs.server.URL+"/reports/performance/freshness", nil)
}

func (r *rules) reportsPathRequested(path string) error {
	rs := r.ensureReports()
	return r.w.doURL(http.MethodGet, rs.server.URL+path, nil)
}

func registerReportSteps(sc *godog.ScenarioContext, r *rules) {
	sc.Step(`^the analytics projector consumes the published events$`, r.projectPublished)
	sc.Step(`^the analytics projector consumes the last analytics "([^"]*)" message again$`, r.projectAgain)
	sc.Step(`^a legacy flat-envelope message arrives on the analytics topic$`, r.legacyFlatEnvelope)
	sc.Step(`^an analytics message of the unrelated type "([^"]*)" arrives$`, r.unrelatedType)
	sc.Step(`^an analytics "TaskPerformanceRecorded" message with undecodable data arrives$`, r.undecodableData)
	sc.Step(`^the analytical database becomes unreachable$`, r.storeDown)
	sc.Step(`^the performance report is requested with query "([^"]*)"$`, r.reportRequested)
	sc.Step(`^the freshness lag is requested$`, r.freshnessRequested)
	sc.Step(`^the reports path "([^"]*)" is requested$`, r.reportsPathRequested)
}
