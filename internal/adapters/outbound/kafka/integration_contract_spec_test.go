package kafka

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"gopkg.in/yaml.v3"

	"github.com/claudioed/labor-performance/internal/adapters/kafka/cloudevents"
)

// The producer side of the integration contract: apis/asyncapi.yaml must
// document, on the integration channel, every CloudEvents type the
// IntegrationPublisher emits (ADR 0013, ADR 0038), each with the `:events:`
// dataschema const. A type the publisher emits but the spec does not list
// is a contract a consumer cannot see.

type specDoc struct {
	Channels map[string]struct {
		Publish *struct {
			Message struct {
				Ref   string `yaml:"$ref"`
				OneOf []struct {
					Ref string `yaml:"$ref"`
				} `yaml:"oneOf"`
			} `yaml:"message"`
		} `yaml:"publish"`
	} `yaml:"channels"`
	Components struct {
		Messages map[string]struct {
			Payload struct {
				Ref string `yaml:"$ref"`
			} `yaml:"payload"`
		} `yaml:"messages"`
		Schemas map[string]struct {
			AllOf []struct {
				Properties map[string]struct {
					Const string `yaml:"const"`
				} `yaml:"properties"`
			} `yaml:"allOf"`
		} `yaml:"schemas"`
	} `yaml:"components"`
}

func lastSegment(ref string) string { return ref[strings.LastIndex(ref, "/")+1:] }

func TestIntegrationChannelDocumentsEveryTypeThePublisherEmits(t *testing.T) {
	raw, err := os.ReadFile(filepath.Join("..", "..", "..", "..", "apis", "asyncapi.yaml"))
	if err != nil {
		t.Fatalf("read apis/asyncapi.yaml: %v", err)
	}
	var doc specDoc
	if err := yaml.Unmarshal(raw, &doc); err != nil {
		t.Fatalf("parse apis/asyncapi.yaml: %v", err)
	}
	ch, ok := doc.Channels[cloudevents.TopicLaborPerformanceEvents]
	if !ok || ch.Publish == nil {
		t.Fatalf("no publish operation on %s", cloudevents.TopicLaborPerformanceEvents)
	}
	refs := []string{}
	if ch.Publish.Message.Ref != "" {
		refs = append(refs, ch.Publish.Message.Ref)
	}
	for _, r := range ch.Publish.Message.OneOf {
		refs = append(refs, r.Ref)
	}
	documented := map[string]string{} // type -> dataschema
	for _, ref := range refs {
		msg := doc.Components.Messages[lastSegment(ref)]
		for _, part := range doc.Components.Schemas[lastSegment(msg.Payload.Ref)].AllOf {
			if typ := part.Properties["type"].Const; typ != "" {
				documented[typ] = part.Properties["dataschema"].Const
			}
		}
	}
	for typ, name := range map[string]string{
		cloudevents.TypeTaskPerformanceRecorded: cloudevents.EventTaskPerformanceRecorded,
		cloudevents.TypeLaborStandardDefined:    cloudevents.EventLaborStandardDefined,
		cloudevents.TypeLaborStandardRevised:    cloudevents.EventLaborStandardRevised,
	} {
		want := cloudevents.DataSchema(cloudevents.StreamEvents, name, 1)
		if got, ok := documented[typ]; !ok || got != want {
			t.Errorf("integration channel documents %s with dataschema %q (present=%v), want %q", typ, got, ok, want)
		}
	}
}
