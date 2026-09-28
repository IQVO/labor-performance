package telemetry_test

import (
	"context"
	"testing"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"

	"github.com/claudioed/labor-performance/internal/adapters/outbound/telemetry"
)

func TestStandardMetrics_CountsByOutcome(t *testing.T) {
	reader := metric.NewManualReader()
	previous := otel.GetMeterProvider()
	otel.SetMeterProvider(metric.NewMeterProvider(metric.WithReader(reader)))
	t.Cleanup(func() { otel.SetMeterProvider(previous) })

	metrics, err := telemetry.NewStandardMetrics()
	if err != nil {
		t.Fatalf("NewStandardMetrics: %v", err)
	}

	ctx := context.Background()
	metrics.StandardDefinitionAccepted(ctx)
	metrics.StandardDefinitionAccepted(ctx)
	metrics.StandardDefinitionRejected(ctx)

	counts := outcomeCounts(t, reader, ctx)

	if counts["accepted"] != 2 {
		t.Errorf("outcome=accepted count = %d, want 2", counts["accepted"])
	}
	if counts["rejected"] != 1 {
		t.Errorf("outcome=rejected count = %d, want 1", counts["rejected"])
	}
}

// outcomeCounts collects the standards-defined counter's per-outcome data
// points from the manual reader.
func outcomeCounts(t *testing.T, reader *metric.ManualReader, ctx context.Context) map[string]int64 {
	t.Helper()
	var collected metricdata.ResourceMetrics
	if err := reader.Collect(ctx, &collected); err != nil {
		t.Fatalf("Collect: %v", err)
	}

	counts := map[string]int64{}
	for _, scope := range collected.ScopeMetrics {
		for _, m := range scope.Metrics {
			if m.Name != "labor_performance.standards.defined" {
				continue
			}
			sum, ok := m.Data.(metricdata.Sum[int64])
			if !ok {
				t.Fatalf("labor_performance.standards.defined is a %T, want an int64 Sum", m.Data)
			}
			for _, point := range sum.DataPoints {
				outcome, ok := point.Attributes.Value("outcome")
				if !ok {
					t.Fatal("data point has no outcome attribute")
				}
				counts[outcome.AsString()] = point.Value
			}
		}
	}
	return counts
}
