//go:build integration

package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"net"
	"net/http"
	"strings"
	"testing"
	"time"

	ce "github.com/cloudevents/sdk-go/v2/event"
	"github.com/jackc/pgx/v5/pgxpool"
	kafkago "github.com/segmentio/kafka-go"
	"github.com/testcontainers/testcontainers-go"
	tckafka "github.com/testcontainers/testcontainers-go/modules/kafka"
	tcpostgres "github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/wait"

	"github.com/claudioed/labor-performance/internal/adapters/kafka/cloudevents"
)

// The end-to-end proof that the shipped binary is really wired (ADR 0034): the
// test boots the REAL entry point (run, in-process) against testcontainers
// Postgres and Kafka with nothing but the environment a deployment sets, and
// drives it only through REST and Kafka. It reads the effects from Postgres
// and from the two produced topics (integration and analytics), replays a
// write and a redelivered TaskCompleted to watch the idempotency, restarts the
// service on the same state, and finally takes the dependencies away to watch
// /readyz follow them.

// containerStartTimeout bounds a testcontainers start.
const containerStartTimeout = 120 * time.Second

func startE2EPostgres(t *testing.T) (*tcpostgres.PostgresContainer, string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), containerStartTimeout)
	defer cancel()
	c, err := tcpostgres.Run(ctx, "postgres:16-alpine",
		tcpostgres.WithDatabase("labor_e2e"), tcpostgres.WithUsername("e2e"), tcpostgres.WithPassword("e2e"),
		testcontainers.WithWaitStrategy(wait.ForLog("database system is ready to accept connections").WithOccurrence(2).WithStartupTimeout(60*time.Second)))
	if err != nil {
		t.Fatalf("start postgres: %v", err)
	}
	t.Cleanup(func() { _ = testcontainers.TerminateContainer(c) })
	url, err := c.ConnectionString(ctx, "sslmode=disable")
	if err != nil {
		t.Fatalf("postgres dsn: %v", err)
	}
	return c, url
}

func startE2EKafka(t *testing.T) (*tckafka.KafkaContainer, []string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), containerStartTimeout)
	defer cancel()
	c, err := tckafka.Run(ctx, "confluentinc/confluent-local:7.6.1", tckafka.WithClusterID(fmt.Sprintf("labor-performance-e2e-%d", time.Now().UnixNano())))
	if err != nil {
		t.Fatalf("start kafka: %v", err)
	}
	t.Cleanup(func() { _ = testcontainers.TerminateContainer(c) })
	brokers, err := c.Brokers(ctx)
	if err != nil {
		t.Fatalf("kafka brokers: %v", err)
	}
	return c, brokers
}

// freeAddr reserves a loopback port for the service's listener.
func freeAddr(t *testing.T) string {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	return l.Addr().String()
}

// topicExists asks the broker for the topic through a metadata request that
// cannot create it (kafka-go's Client.Metadata never auto-creates).
func topicExists(t *testing.T, brokers []string, topic string) bool {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	resp, err := (&kafkago.Client{Addr: kafkago.TCP(brokers...)}).Metadata(ctx, &kafkago.MetadataRequest{Topics: []string{topic}})
	if err != nil {
		t.Fatalf("metadata: %v", err)
	}
	for _, tp := range resp.Topics {
		if tp.Name == topic && tp.Error == nil {
			return true
		}
	}
	return false
}

// createTopic creates a one-partition topic and waits for its leader.
func createTopic(t *testing.T, brokers []string, topic string) {
	t.Helper()
	conn, err := kafkago.Dial("tcp", brokers[0])
	if err != nil {
		t.Fatalf("dial broker: %v", err)
	}
	defer conn.Close()
	if err := conn.CreateTopics(kafkago.TopicConfig{Topic: topic, NumPartitions: 1, ReplicationFactor: 1}); err != nil {
		t.Fatalf("create topic %s: %v", topic, err)
	}
	for deadline := time.Now().Add(30 * time.Second); time.Now().Before(deadline); time.Sleep(200 * time.Millisecond) {
		if parts, err := conn.ReadPartitions(topic); err == nil && len(parts) == 1 && parts[0].Leader.ID != 0 {
			return
		}
	}
	t.Fatalf("topic %s never got a leader", topic)
}

// consume reads exactly n messages of the one-partition topic from the start,
// bounded by a deadline.
func consume(t *testing.T, brokers []string, topic string, n int) []kafkago.Message {
	t.Helper()
	r := kafkago.NewReader(kafkago.ReaderConfig{Brokers: brokers, Topic: topic, Partition: 0, MinBytes: 1, MaxBytes: 1 << 20, MaxWait: 200 * time.Millisecond})
	defer r.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	if err := r.SetOffset(kafkago.FirstOffset); err != nil {
		t.Fatal(err)
	}
	var out []kafkago.Message
	for len(out) < n {
		m, err := r.ReadMessage(ctx)
		if err != nil {
			t.Fatalf("read %s: got %d of %d messages: %v", topic, len(out), n, err)
		}
		out = append(out, m)
	}
	return out
}

// noMoreMessages fails if the topic holds more than want messages after a
// settling period (nothing was duplicated).
func noMoreMessages(t *testing.T, brokers []string, topic string, want int) {
	t.Helper()
	r := kafkago.NewReader(kafkago.ReaderConfig{Brokers: brokers, Topic: topic, Partition: 0, MinBytes: 1, MaxBytes: 1 << 20, MaxWait: 200 * time.Millisecond})
	defer r.Close()
	if err := r.SetOffset(kafkago.FirstOffset); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	got := 0
	for {
		if _, err := r.ReadMessage(ctx); err != nil {
			break
		}
		got++
	}
	if got != want {
		t.Fatalf("%s holds %d messages, want exactly %d", topic, got, want)
	}
}

// produceTaskCompleted writes fulfillment-execution's TaskCompleted onto the
// shared fulfillment topic exactly as it appears on the wire (CloudEvents 1.0,
// structured mode, the full cross-service type).
func produceTaskCompleted(t *testing.T, brokers []string, eventID string, data map[string]any) {
	t.Helper()
	e := ce.New(cloudevents.SpecVersion)
	e.SetID(eventID)
	e.SetSource("/warehouse/fulfillment-execution")
	e.SetType(cloudevents.TypeFulfillmentTaskCompleted)
	e.SetSubject(fmt.Sprint(data["task_id"]))
	e.SetTime(time.Now().UTC())
	e.SetDataSchema("urn:warehouse:fulfillment-execution:events:TaskCompleted:v1")
	if err := e.SetData(cloudevents.DataContentType, data); err != nil {
		t.Fatalf("set data: %v", err)
	}
	body, err := json.Marshal(e)
	if err != nil {
		t.Fatalf("marshal TaskCompleted: %v", err)
	}
	w := &kafkago.Writer{Addr: kafkago.TCP(brokers...), Topic: cloudevents.TopicFulfillmentEvents, RequiredAcks: kafkago.RequireAll}
	defer w.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	msg := kafkago.Message{Key: []byte(eventID), Value: body, Headers: []kafkago.Header{cloudevents.ContentTypeHeader()}}
	// A topic created a moment ago can answer "unknown topic" until the
	// metadata reaches every broker: retry within the bound.
	var werr error
	for ctx.Err() == nil {
		if werr = w.WriteMessages(ctx, msg); werr == nil {
			return
		}
		time.Sleep(300 * time.Millisecond)
	}
	t.Fatalf("produce TaskCompleted: %v", werr)
}

// service is one running instance of the real entry point.
type service struct {
	t      *testing.T
	base   string
	cancel context.CancelFunc
	done   chan error
}

// e2eEnv is the environment a deployment sets.
type e2eEnv struct {
	dbURL   string
	brokers []string
	addr    string
	group   string
}

func newE2EEnv(t *testing.T, dbURL string, brokers []string) e2eEnv {
	t.Helper()
	return e2eEnv{dbURL: dbURL, brokers: brokers, addr: freeAddr(t), group: fmt.Sprintf("labor-performance-e2e-%d", time.Now().UnixNano())}
}

func (e e2eEnv) apply(t *testing.T) {
	t.Helper()
	t.Setenv("DATABASE_URL", e.dbURL)
	t.Setenv("MIGRATIONS_DATABASE_URL", "")
	t.Setenv("MIGRATIONS_PATH", "../../migrations")
	t.Setenv("KAFKA_BROKERS", strings.Join(e.brokers, ","))
	t.Setenv("KAFKA_CONSUMER_GROUP", e.group)
	t.Setenv("EVENT_PUBLISHER", "kafka")
	t.Setenv("HTTP_ADDR", e.addr)
	t.Setenv("OUTBOX_RELAY_INTERVAL", "1s")
	t.Setenv("LOG_LEVEL", "warn")
}

// start boots run in-process and waits until the listener answers /healthz.
func (e e2eEnv) start(t *testing.T) *service {
	t.Helper()
	e.apply(t)
	ctx, cancel := context.WithCancel(context.Background())
	s := &service{t: t, base: "http://" + e.addr, cancel: cancel, done: make(chan error, 1)}
	go func() { s.done <- run(ctx) }()
	t.Cleanup(s.stopQuietly)
	waitFor(t, 90*time.Second, "the service to listen on "+e.addr, func() bool {
		select {
		case err := <-s.done:
			t.Fatalf("run returned while booting: %v", err)
		default:
		}
		status, _ := s.try(http.MethodGet, "/healthz", "", nil)
		return status == http.StatusOK
	})
	return s
}

// stop cancels run and requires a clean shutdown within a bound.
func (s *service) stop() {
	s.t.Helper()
	s.cancel()
	select {
	case err := <-s.done:
		if err != nil {
			s.t.Fatalf("shutdown returned %v", err)
		}
	case <-time.After(60 * time.Second):
		s.t.Fatal("run did not return after the shutdown")
	}
	s.done <- nil // let the cleanup find it stopped
}

func (s *service) stopQuietly() {
	s.cancel()
	select {
	case <-s.done:
	case <-time.After(60 * time.Second):
	}
}

// try performs a request and returns -1 when the connection fails.
func (s *service) try(method, path, body string, headers map[string]string) (int, string) {
	req, err := http.NewRequestWithContext(context.Background(), method, s.base+path, strings.NewReader(body))
	if err != nil {
		return -1, err.Error()
	}
	if body != "" {
		req.Header.Set("Content-Type", "application/json")
	}
	for k, v := range headers {
		req.Header.Set(k, v)
	}
	resp, err := (&http.Client{Timeout: 10 * time.Second}).Do(req)
	if err != nil {
		return -1, err.Error()
	}
	defer resp.Body.Close()
	raw, _ := io.ReadAll(resp.Body)
	return resp.StatusCode, string(raw)
}

func (s *service) call(method, path, body string, want int) string {
	s.t.Helper()
	return s.callWith(method, path, body, nil, want)
}

func (s *service) callWith(method, path, body string, headers map[string]string, want int) string {
	s.t.Helper()
	status, raw := s.try(method, path, body, headers)
	if status != want {
		s.t.Fatalf("%s %s = %d %s, want %d", method, path, status, raw, want)
	}
	return raw
}

func waitFor(t *testing.T, timeout time.Duration, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(100 * time.Millisecond)
	}
	t.Fatalf("timed out after %s waiting for %s", timeout, what)
}

// pgView reads the service's own database.
type pgView struct {
	t    *testing.T
	pool *pgxpool.Pool
}

func newPGView(t *testing.T, url string) pgView {
	t.Helper()
	pool, err := pgxpool.New(context.Background(), url)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	return pgView{t: t, pool: pool}
}

func (v pgView) count(query string, args ...any) int {
	v.t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	var n int
	if err := v.pool.QueryRow(ctx, query, args...).Scan(&n); err != nil {
		v.t.Fatalf("%s: %v", query, err)
	}
	return n
}

func (v pgView) unpublished() int {
	return v.count(`SELECT count(*) FROM outbox_events WHERE published_at IS NULL`)
}

// envelope holds the CloudEvents context attributes of a published message,
// plus its data.
type envelope struct {
	SpecVersion     string          `json:"specversion"`
	ID              string          `json:"id"`
	Source          string          `json:"source"`
	Type            string          `json:"type"`
	Subject         string          `json:"subject"`
	Time            string          `json:"time"`
	DataContentType string          `json:"datacontenttype"`
	DataSchema      string          `json:"dataschema"`
	Data            json.RawMessage `json:"data"`
}

func decodeEnvelope(t *testing.T, m kafkago.Message) envelope {
	t.Helper()
	var e envelope
	if err := json.Unmarshal(m.Value, &e); err != nil {
		t.Fatalf("not a CloudEvent: %v: %s", err, m.Value)
	}
	return e
}

// assertEnvelope checks every context attribute of a message of this service.
func assertEnvelope(t *testing.T, m kafkago.Message, entity, eventName, stream, subject, key string) envelope {
	t.Helper()
	e := decodeEnvelope(t, m)
	wantType := "com.warehouse.wes.labor-performance." + entity + "." + eventName
	schema := fmt.Sprintf("urn:warehouse:labor-performance:%s:%s:v1", stream, eventName)
	if e.SpecVersion != "1.0" || e.Type != wantType || e.Source != "/warehouse/labor-performance" ||
		e.DataSchema != schema || e.DataContentType != "application/json" || e.Subject != subject || e.ID == "" {
		t.Fatalf("envelope = %+v, want 1.0 %s from /warehouse/labor-performance (%s) about %s", e, wantType, schema, subject)
	}
	if _, err := time.Parse(time.RFC3339Nano, e.Time); err != nil {
		t.Fatalf("time %q is not RFC 3339: %v", e.Time, err)
	}
	if string(m.Key) != key {
		t.Fatalf("kafka key = %q, want %q", m.Key, key)
	}
	var ct string
	for _, h := range m.Headers {
		if h.Key == "content-type" {
			ct = string(h.Value)
		}
	}
	if ct != "application/cloudevents+json; charset=UTF-8" {
		t.Fatalf("content-type header = %q", ct)
	}
	return e
}

func readyz(s *service) (int, string) { return s.try(http.MethodGet, "/readyz", "", nil) }

func waitReadyzStatus(t *testing.T, s *service, want int, what string) string {
	t.Helper()
	var body string
	waitFor(t, 60*time.Second, what, func() bool {
		var status int
		status, body = readyz(s)
		return status == want
	})
	return body
}

// Phase 1: the topics do not exist yet (a fresh cluster before anything
// published). Readiness needs Postgres plus a broker answering metadata, NOT
// topics, so the service is ready at once, and it creates no topic itself.
func phaseTopicsMissing(t *testing.T, s *service, brokers []string) {
	t.Helper()
	body := waitReadyzStatus(t, s, http.StatusOK, "/readyz to be 200 although no topic exists")
	if !strings.Contains(body, "ready") {
		t.Fatalf("readyz = %s", body)
	}
	for _, topic := range []string{cloudevents.TopicLaborPerformanceEvents, cloudevents.TopicLaborPerformanceAnalytics, cloudevents.TopicFulfillmentEvents} {
		if topicExists(t, brokers, topic) {
			t.Fatalf("topic %s exists: the service must never create topics itself", topic)
		}
	}
	// The topics appear as the cluster broker would create them on first write.
	for _, topic := range []string{cloudevents.TopicLaborPerformanceEvents, cloudevents.TopicLaborPerformanceAnalytics, cloudevents.TopicFulfillmentEvents} {
		createTopic(t, brokers, topic)
	}
	if status, body := readyz(s); status != http.StatusOK {
		t.Fatalf("readyz after the topics appeared = %d %s, want 200", status, body)
	}
}

const (
	standardBody = `{"taskType":"PICK","expectedSeconds":60}`
	standardKey  = "e2e-standard-pick"
)

// Phase 2: the write API (REST) and the inbound event (Kafka). One standard,
// one TaskCompleted of 90 s against it.
func phaseWrites(t *testing.T, s *service, brokers []string) {
	t.Helper()
	body := s.callWith(http.MethodPost, "/standards", standardBody, map[string]string{"Idempotency-Key": standardKey}, http.StatusCreated)
	if !strings.Contains(body, `"taskType":"PICK"`) || !strings.Contains(body, `"expectedSeconds":60`) {
		t.Fatalf("POST /standards = %s", body)
	}
	if got := s.call(http.MethodGet, "/standards/PICK", "", http.StatusOK); !strings.Contains(got, `"expectedSeconds":60`) {
		t.Fatalf("GET /standards/PICK = %s", got)
	}

	produceTaskCompleted(t, brokers, "e2e-evt-1", map[string]any{
		"task_id": "e2e-task-1", "station_id": "station-1", "work_unit_id": "wu-1",
		"associate_id": "A-1", "duration_seconds": 90, "task_type": "PICK",
	})
	waitFor(t, 60*time.Second, "the consumer to record the TaskCompleted", func() bool {
		status, raw := s.try(http.MethodGet, "/associates/A-1/scorecard", "", nil)
		return status == http.StatusOK && strings.Contains(raw, `"taskCount":1`)
	})
	var sc struct {
		TaskCount         int      `json:"taskCount"`
		MeanEfficiencyPct *float64 `json:"meanEfficiencyPct"`
	}
	if err := json.Unmarshal([]byte(s.call(http.MethodGet, "/associates/A-1/scorecard", "", http.StatusOK)), &sc); err != nil {
		t.Fatal(err)
	}
	// 60 s standard / 90 s actual.
	if sc.TaskCount != 1 || sc.MeanEfficiencyPct == nil || math.Abs(*sc.MeanEfficiencyPct-66.6667) > 0.01 {
		t.Fatalf("scorecard = %+v (efficiency %v), want 1 task at ~66.67%%", sc, sc.MeanEfficiencyPct)
	}
}

// Phase 3: the rows are in Postgres, and the relay drained every one of them.
func phaseRows(t *testing.T, db pgView) {
	t.Helper()
	for _, q := range []string{
		`SELECT count(*) FROM labor_standards WHERE task_type = 'PICK' AND expected_seconds = 60 AND effective_to IS NULL`,
		`SELECT count(*) FROM task_performances WHERE event_id = 'e2e-evt-1' AND task_id = 'e2e-task-1' AND associate_id = 'A-1' AND actual_seconds = 90 AND standard_seconds_at_completion = 60`,
		`SELECT count(*) FROM processed_events WHERE event_id = 'e2e-evt-1'`,
	} {
		if n := db.count(q); n != 1 {
			t.Fatalf("%s = %d, want 1", q, n)
		}
	}
	// The standard raises LaborStandardDefined (analytics only); the recorded
	// performance raises TaskPerformanceRecorded on BOTH topics.
	if n := db.count(`SELECT count(*) FROM outbox_events`); n != 3 {
		t.Fatalf("outbox rows = %d, want 3", n)
	}
	waitFor(t, 60*time.Second, "the relay to publish every outbox row", func() bool { return db.unpublished() == 0 })
	for topic, want := range map[string]int{cloudevents.TopicLaborPerformanceEvents: 1, cloudevents.TopicLaborPerformanceAnalytics: 2} {
		if n := db.count(`SELECT count(*) FROM outbox_events WHERE topic = $1`, topic); n != want {
			t.Fatalf("outbox rows for %s = %d, want %d", topic, n, want)
		}
	}
}

// Phase 4: the CloudEvents on both output topics, byte-exact envelope.
func phaseTopics(t *testing.T, brokers []string) {
	t.Helper()
	ev := consume(t, brokers, cloudevents.TopicLaborPerformanceEvents, 1)
	perf := assertEnvelope(t, ev[0], "performance", "TaskPerformanceRecorded", "events", "A-1", "A-1")
	var data struct {
		TaskID        string   `json:"task_id"`
		AssociateID   string   `json:"associate_id"`
		TaskType      string   `json:"task_type"`
		ActualSeconds int64    `json:"actual_seconds"`
		EfficiencyPct *float64 `json:"efficiency_pct"`
	}
	if err := json.Unmarshal(perf.Data, &data); err != nil || data.TaskID != "e2e-task-1" || data.AssociateID != "A-1" ||
		data.TaskType != "PICK" || data.ActualSeconds != 90 || data.EfficiencyPct == nil || math.Abs(*data.EfficiencyPct-66.6667) > 0.01 {
		t.Fatalf("TaskPerformanceRecorded data = %s (%v)", perf.Data, err)
	}

	an := consume(t, brokers, cloudevents.TopicLaborPerformanceAnalytics, 2)
	defined := assertEnvelope(t, an[0], "standard", "LaborStandardDefined", "analytics", decodeStandardID(t, an[0]), "PICK")
	var def struct {
		TaskType        string `json:"task_type"`
		ExpectedSeconds int64  `json:"expected_seconds"`
	}
	if err := json.Unmarshal(defined.Data, &def); err != nil || def.TaskType != "PICK" || def.ExpectedSeconds != 60 {
		t.Fatalf("LaborStandardDefined data = %s (%v)", defined.Data, err)
	}
	aPerf := assertEnvelope(t, an[1], "performance", "TaskPerformanceRecorded", "analytics", "A-1", "PICK")
	// One occurrence, one CloudEvents id... on its two topics (each topic's
	// publisher mints its own id per message; they only have to be set and unique).
	if aPerf.ID == perf.ID && aPerf.ID == defined.ID {
		t.Fatalf("ids collide: %s", aPerf.ID)
	}
}

func decodeStandardID(t *testing.T, m kafkago.Message) string {
	t.Helper()
	var data struct {
		StandardID string `json:"standard_id"`
	}
	if err := json.Unmarshal(decodeEnvelope(t, m).Data, &data); err != nil || data.StandardID == "" {
		t.Fatalf("LaborStandardDefined has no standard_id: %s (%v)", m.Value, err)
	}
	return data.StandardID
}

// Phase 5: replaying a creation under the same Idempotency-Key returns the
// stored response and adds no row and no event; the same key with another
// body, a creation without a key and a NUL in the body are refused; a
// redelivered TaskCompleted records nothing twice.
func phaseReplay(t *testing.T, s *service, db pgView, brokers []string) {
	t.Helper()
	first := s.callWith(http.MethodPost, "/standards", standardBody, map[string]string{"Idempotency-Key": standardKey}, http.StatusCreated)
	if first == "" {
		t.Fatal("empty replay response")
	}
	if body := s.callWith(http.MethodPost, "/standards", `{"taskType":"PICK","expectedSeconds":61}`, map[string]string{"Idempotency-Key": standardKey}, http.StatusConflict); !strings.Contains(body, "idempotency-key-reused") {
		t.Fatalf("the same key with another body = %s, want idempotency-key-reused", body)
	}
	if body := s.call(http.MethodPost, "/standards", standardBody, http.StatusBadRequest); !strings.Contains(body, "idempotency-key-required") {
		t.Fatalf("a creation without a key = %s, want idempotency-key-required", body)
	}
	if body := s.callWith(http.MethodPost, "/standards", `{"taskType":"PI\u0000CK","expectedSeconds":60}`, map[string]string{"Idempotency-Key": "e2e-nul"}, http.StatusBadRequest); !strings.Contains(body, "malformed-request-body") {
		t.Fatalf("a NUL in the body = %s, want malformed-request-body (never a 500)", body)
	}
	if n := db.count(`SELECT count(*) FROM idempotency_keys WHERE key = 'e2e-nul'`); n != 0 {
		t.Fatalf("the NUL request consumed its Idempotency-Key (%d rows)", n)
	}

	// The same CloudEvents id delivered again (at-least-once redelivery).
	produceTaskCompleted(t, brokers, "e2e-evt-1", map[string]any{
		"task_id": "e2e-task-1", "station_id": "station-1", "work_unit_id": "wu-1",
		"associate_id": "A-1", "duration_seconds": 90, "task_type": "PICK",
	})
	time.Sleep(3 * time.Second) // let the consumer see it
	if n := db.count(`SELECT count(*) FROM labor_standards`); n != 1 {
		t.Fatalf("labor_standards rows after the replay = %d, want 1", n)
	}
	if n := db.count(`SELECT count(*) FROM task_performances`); n != 1 {
		t.Fatalf("task_performances rows after the redelivery = %d, want 1", n)
	}
	if n := db.count(`SELECT count(*) FROM outbox_events`); n != 3 {
		t.Fatalf("outbox rows after the replay = %d, want still 3", n)
	}
	noMoreMessages(t, brokers, cloudevents.TopicLaborPerformanceEvents, 1)
	noMoreMessages(t, brokers, cloudevents.TopicLaborPerformanceAnalytics, 2)
}

// Phase 6: restart the real entry point in-process on the same database and
// broker (same consumer group): nothing is lost, nothing is published twice.
func phaseRestart(t *testing.T, e e2eEnv, s *service, db pgView, brokers []string) *service {
	t.Helper()
	s.stop()
	s2 := e.start(t)
	if got := s2.call(http.MethodGet, "/standards/PICK", "", http.StatusOK); !strings.Contains(got, `"expectedSeconds":60`) {
		t.Fatalf("after the restart GET /standards/PICK = %s", got)
	}
	produceTaskCompleted(t, brokers, "e2e-evt-2", map[string]any{
		"task_id": "e2e-task-2", "station_id": "station-1", "work_unit_id": "wu-2",
		"associate_id": "A-1", "duration_seconds": 30, "task_type": "PICK",
	})
	waitFor(t, 60*time.Second, "the restarted consumer to record the second TaskCompleted", func() bool {
		status, raw := s2.try(http.MethodGet, "/associates/A-1/scorecard", "", nil)
		return status == http.StatusOK && strings.Contains(raw, `"taskCount":2`)
	})
	waitFor(t, 60*time.Second, "the relay to publish after the restart", func() bool { return db.unpublished() == 0 })
	msgs := consume(t, brokers, cloudevents.TopicLaborPerformanceEvents, 2)
	if a, b := decodeEnvelope(t, msgs[0]).ID, decodeEnvelope(t, msgs[1]).ID; a == b {
		t.Fatalf("both integration messages carry the id %s: an event was published twice", a)
	}
	noMoreMessages(t, brokers, cloudevents.TopicLaborPerformanceEvents, 2)
	assertEnvelope(t, msgs[1], "performance", "TaskPerformanceRecorded", "events", "A-1", "A-1")
	return s2
}

// Phase 7: /readyz follows the dependencies. Postgres goes first (Kafka still
// up): only postgres is named. Then Kafka goes: kafka is named too. /healthz
// stays liveness and answers 200 throughout.
func phaseDependenciesGoAway(t *testing.T, s *service, pg *tcpostgres.PostgresContainer, kc *tckafka.KafkaContainer) {
	t.Helper()
	stopTimeout := 10 * time.Second
	if err := pg.Stop(context.Background(), &stopTimeout); err != nil {
		t.Fatalf("stop postgres: %v", err)
	}
	body := waitReadyzStatus(t, s, http.StatusServiceUnavailable, "/readyz to turn 503 once Postgres is gone")
	if !strings.Contains(body, "dependency-unavailable") || !strings.Contains(body, "postgres") || strings.Contains(body, "kafka") {
		t.Fatalf("readyz with Postgres down = %s, want dependency-unavailable naming only postgres", body)
	}
	if err := kc.Stop(context.Background(), &stopTimeout); err != nil {
		t.Fatalf("stop kafka: %v", err)
	}
	waitFor(t, 60*time.Second, "/readyz to name kafka as well", func() bool {
		_, body := readyz(s)
		return strings.Contains(body, "postgres") && strings.Contains(body, "kafka")
	})
	if status, body := s.try(http.MethodGet, "/healthz", "", nil); status != http.StatusOK {
		t.Fatalf("healthz with every dependency down = %d %s, want 200 (liveness only)", status, body)
	}
}

func TestRunOnRealPostgresAndKafka(t *testing.T) {
	pg, dbURL := startE2EPostgres(t)
	kc, brokers := startE2EKafka(t)
	e := newE2EEnv(t, dbURL, brokers)
	db := newPGView(t, dbURL)

	s := e.start(t)
	phaseTopicsMissing(t, s, brokers)
	phaseWrites(t, s, brokers)
	phaseRows(t, db)
	phaseTopics(t, brokers)
	phaseReplay(t, s, db, brokers)
	s = phaseRestart(t, e, s, db, brokers)
	phaseDependenciesGoAway(t, s, pg, kc)
}
