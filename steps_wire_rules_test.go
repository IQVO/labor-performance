package main_test

// Wire-contract steps: the published domain events are encoded by the real
// analytics and integration encoders (the same ones the transactional outbox
// stores), so the scenarios can pin what domain-events.md documents -- topic,
// partition key, subject, CloudEvents attributes and payload fields -- without
// a broker.

import (
	"context"
	"encoding/json"
	"fmt"
	"reflect"
	"strings"
	"time"

	ce "github.com/cloudevents/sdk-go/v2/event"
	"github.com/cucumber/godog"

	"github.com/claudioed/labor-performance/internal/adapters/kafka/cloudevents"
	outboundkafka "github.com/claudioed/labor-performance/internal/adapters/outbound/kafka"
	"github.com/claudioed/labor-performance/internal/domain/shared"
)

type wireMsg struct {
	stream string // "analytics" or "integration"
	name   string // PascalCase event name, last segment of the CloudEvents type
	enc    outboundkafka.Encoded
	evt    ce.Event
	data   map[string]any
}

type wireState struct {
	analytics   *outboundkafka.AnalyticsPublisher
	integration *outboundkafka.IntegrationPublisher
	seq         int
	encoded     int // domain events already encoded
	msgs        []wireMsg
}

func newWireState() *wireState {
	ws := &wireState{}
	newID := func() string {
		ws.seq++
		return fmt.Sprintf("ce-%03d", ws.seq)
	}
	ws.analytics = &outboundkafka.AnalyticsPublisher{NewID: newID}
	ws.integration = &outboundkafka.IntegrationPublisher{NewID: newID}
	return ws
}

// encodeNew runs every domain event published since the last call through
// both encoders, in publication order.
func (r *rules) encodeNew() error {
	events := r.w.h.published.snapshot()
	for ; r.wire.encoded < len(events); r.wire.encoded++ {
		ev := events[r.wire.encoded]
		for _, enc := range []struct {
			stream string
			encode func(context.Context, ...shared.DomainEvent) ([]outboundkafka.Encoded, error)
		}{
			{"analytics", r.wire.analytics.Encode},
			{"integration", r.wire.integration.Encode},
		} {
			out, err := enc.encode(context.Background(), ev)
			if err != nil {
				return fmt.Errorf("encode %s %s: %w", enc.stream, ev.EventName(), err)
			}
			for _, m := range out {
				decoded, err := cloudevents.Decode(m.Value)
				if err != nil {
					return fmt.Errorf("encoded %s message is not a CloudEvent: %w", enc.stream, err)
				}
				data := map[string]any{}
				if err := decoded.DataAs(&data); err != nil {
					return fmt.Errorf("decode %s data: %w", enc.stream, err)
				}
				name := m.EventType[strings.LastIndex(m.EventType, ".")+1:]
				r.wire.msgs = append(r.wire.msgs, wireMsg{stream: enc.stream, name: name, enc: m, evt: decoded, data: data})
			}
		}
	}
	return nil
}

func (r *rules) messages(stream, name string) ([]wireMsg, error) {
	if err := r.encodeNew(); err != nil {
		return nil, err
	}
	var out []wireMsg
	for _, m := range r.wire.msgs {
		if m.stream == stream && m.name == name {
			out = append(out, m)
		}
	}
	return out, nil
}

func (r *rules) lastMessage(stream, name string) (wireMsg, error) {
	msgs, err := r.messages(stream, name)
	if err != nil {
		return wireMsg{}, err
	}
	if len(msgs) == 0 {
		return wireMsg{}, fmt.Errorf("no %s %s message was encoded", stream, name)
	}
	return msgs[len(msgs)-1], nil
}

func (r *rules) messageRouting(stream, name, topic, key, subject string) error {
	m, err := r.lastMessage(stream, name)
	if err != nil {
		return err
	}
	if m.enc.Topic != topic || string(m.enc.Key) != key || m.evt.Subject() != subject {
		return fmt.Errorf("%s %s message is on topic %q key %q subject %q, want %q %q %q",
			stream, name, m.enc.Topic, string(m.enc.Key), m.evt.Subject(), topic, key, subject)
	}
	return nil
}

func (r *rules) messageType(stream, name, eventType, schema string) error {
	m, err := r.lastMessage(stream, name)
	if err != nil {
		return err
	}
	if m.evt.Type() != eventType || m.evt.DataSchema() != schema {
		return fmt.Errorf("%s %s message has type %q schema %q, want %q %q", stream, name, m.evt.Type(), m.evt.DataSchema(), eventType, schema)
	}
	return nil
}

func (r *rules) messageEnvelope(stream, name, source, at string) error {
	m, err := r.lastMessage(stream, name)
	if err != nil {
		return err
	}
	want, err := parseInstant(at)
	if err != nil {
		return err
	}
	if m.evt.SpecVersion() != "1.0" || m.evt.Source() != source || !m.evt.Time().Equal(want) || m.evt.DataContentType() != "application/json" || m.evt.ID() == "" {
		return fmt.Errorf("%s %s envelope: specversion %q source %q time %s datacontenttype %q id %q",
			stream, name, m.evt.SpecVersion(), m.evt.Source(), m.evt.Time(), m.evt.DataContentType(), m.evt.ID())
	}
	if m.evt.Time().Location() != time.UTC && m.evt.Time().UTC() != m.evt.Time() {
		return fmt.Errorf("%s %s time is not UTC: %s", stream, name, m.evt.Time())
	}
	var contentType string
	for _, h := range m.enc.Headers {
		if h.Key == "content-type" {
			contentType = string(h.Value)
		}
	}
	if contentType != "application/cloudevents+json; charset=UTF-8" {
		return fmt.Errorf("%s %s content-type header is %q", stream, name, contentType)
	}
	return nil
}

func (r *rules) messageField(stream, name, field, literal string) error {
	m, err := r.lastMessage(stream, name)
	if err != nil {
		return err
	}
	got, present := m.data[field]
	if !present {
		return fmt.Errorf("%s %s data has no %q: %v", stream, name, field, m.data)
	}
	var want any
	if err := json.Unmarshal([]byte(literal), &want); err != nil {
		return err
	}
	if !jsonEqual(got, want) {
		return fmt.Errorf("%s %s data %q is %v, want %v", stream, name, field, got, want)
	}
	return nil
}

func (r *rules) messageNoField(stream, name, field string) error {
	m, err := r.lastMessage(stream, name)
	if err != nil {
		return err
	}
	if got, present := m.data[field]; present {
		return fmt.Errorf("%s %s data must omit %q, got %v", stream, name, field, got)
	}
	return nil
}

func (r *rules) messageCount(count int, stream, name string) error {
	msgs, err := r.messages(stream, name)
	if err != nil {
		return err
	}
	if len(msgs) != count {
		return fmt.Errorf("%d %s %s message(s) encoded, want %d", len(msgs), stream, name, count)
	}
	return nil
}

func (r *rules) sameDataDifferentIds() error {
	a, err := r.lastMessage("analytics", "TaskPerformanceRecorded")
	if err != nil {
		return err
	}
	i, err := r.lastMessage("integration", "TaskPerformanceRecorded")
	if err != nil {
		return err
	}
	if !reflect.DeepEqual(a.data, i.data) {
		return fmt.Errorf("analytics data %v differs from integration data %v", a.data, i.data)
	}
	if a.evt.ID() == i.evt.ID() {
		return fmt.Errorf("both messages carry id %q; each is a separate CloudEvent", a.evt.ID())
	}
	return nil
}

func registerWireSteps(sc *godog.ScenarioContext, r *rules) {
	const stream = `(analytics|integration)`
	sc.Step(`^the `+stream+` "([^"]*)" message is on topic "([^"]*)" with key "([^"]*)" and subject "([^"]*)"$`, r.messageRouting)
	sc.Step(`^the `+stream+` "([^"]*)" message has type "([^"]*)" and data schema "([^"]*)"$`, r.messageType)
	sc.Step(`^the `+stream+` "([^"]*)" message is a CloudEvents 1.0 event from "([^"]*)" occurring at "([^"]*)"$`, r.messageEnvelope)
	sc.Step(`^the `+stream+` "([^"]*)" message data field "([^"]*)" is (.+)$`, r.messageField)
	sc.Step(`^the `+stream+` "([^"]*)" message data has no field "([^"]*)"$`, r.messageNoField)
	sc.Step(`^(\d+) `+stream+` "([^"]*)" messages? (?:was|were) encoded$`, r.messageCount)
	sc.Step(`^the analytics and integration "TaskPerformanceRecorded" messages carry the same data under different ids$`, r.sameDataDifferentIds)
}
