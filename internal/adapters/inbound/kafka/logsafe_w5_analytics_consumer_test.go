package kafka

import (
	"context"
	"encoding/json"
	"log/slog"
	"testing"

	"github.com/claudioed/labor-performance/internal/adapters/kafka/cloudevents"
)

// TestAnalyticsConsumerLogsNoEventDerivedLineBreaks feeds the consumer a payload that is not a CloudEvent and a
// CloudEvent of a consumed type whose id, subject and data carry CR/LF and
// whose data does not decode: no log attribute may carry the line break.
func TestAnalyticsConsumerLogsNoEventDerivedLineBreaks(t *testing.T) {
	rec := newCRLFRecorder()
	c := &AnalyticsConsumer{Logger: slog.New(rec)}
	ctx := context.Background()
	_ = c.HandleMessage(ctx, []byte("not a CloudEvent"+crlfForged))
	b, err := json.Marshal(map[string]any{
		"specversion": "1.0", "id": "id" + crlfForged, "type": cloudevents.TypeTaskPerformanceRecorded, "source": "urn:logsafe",
		"subject": "subject" + crlfForged, "datacontenttype": "application/json",
		"data": []string{"not an object" + crlfForged},
	})
	if err != nil {
		t.Fatal(err)
	}
	_ = c.HandleMessage(ctx, b)
	rec.assertNoLineBreaks(t, 2)
}
