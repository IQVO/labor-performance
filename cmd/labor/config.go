package main

import (
	"errors"
	"fmt"
	"os"
	"strings"
)

// Boot errors of the mandatory configuration. Each names its variable
// (ADR 0034): the service runs on real Postgres and real Kafka, with no
// default and no degraded mode behind either.
var (
	errNoDatabase      = errors.New("DATABASE_URL is required: this context runs on real Postgres")
	errNoBrokers       = errors.New("KAFKA_BROKERS is required: this context runs on real Kafka")
	errNoConsumerGroup = errors.New("KAFKA_CONSUMER_GROUP is required: this context runs on real Kafka")
)

// bootConfig is the validated mandatory configuration of cmd/labor.
type bootConfig struct {
	databaseURL string
	// migrationsDatabaseURL is a DIRECT (non-pooled, session-mode) Postgres
	// connection string used ONLY for the golang-migrate startup step; it
	// falls back to databaseURL (see buildPersistence's doc comment and ADR
	// 0020-migrations-direct-postgres-connection.md).
	migrationsDatabaseURL string
	migrationsPath        string
	kafkaBrokers          []string
	consumerGroup         string
}

// parseBrokers splits KAFKA_BROKERS (comma separated), trimming blanks. It
// returns nil when nothing is configured: there is no localhost default.
func parseBrokers(raw string) []string {
	var brokers []string
	for _, b := range strings.Split(raw, ",") {
		if b = strings.TrimSpace(b); b != "" {
			brokers = append(brokers, b)
		}
	}
	return brokers
}

// checkEventPublisher validates EVENT_PUBLISHER. The only publisher is the
// Kafka-backed transactional outbox: unset and "kafka" are accepted (the
// variable is kept because the chart and warehouse-infra set it), "log" and
// anything else is a boot error naming the variable.
func checkEventPublisher(raw string) error {
	if raw == "" || strings.EqualFold(raw, "kafka") {
		return nil
	}
	if strings.EqualFold(raw, "log") {
		return errors.New("EVENT_PUBLISHER=log is not supported: this context publishes to real Kafka")
	}
	return fmt.Errorf("unknown EVENT_PUBLISHER %q: this context publishes to real Kafka (want kafka)", raw)
}

// loadBootConfig reads and validates the mandatory environment of cmd/labor.
// Every problem is reported at once, each naming its variable, so the
// process fails fast at boot, before it opens a connection or binds the
// listener.
func loadBootConfig() (bootConfig, error) {
	databaseURL := os.Getenv("DATABASE_URL")
	cfg := bootConfig{
		databaseURL:           databaseURL,
		migrationsDatabaseURL: getenv("MIGRATIONS_DATABASE_URL", databaseURL),
		migrationsPath:        getenv("MIGRATIONS_PATH", "migrations"),
		kafkaBrokers:          parseBrokers(os.Getenv("KAFKA_BROKERS")),
		consumerGroup:         strings.TrimSpace(os.Getenv("KAFKA_CONSUMER_GROUP")),
	}
	var errs []error
	if cfg.databaseURL == "" {
		errs = append(errs, errNoDatabase)
	}
	if len(cfg.kafkaBrokers) == 0 {
		errs = append(errs, errNoBrokers)
	}
	if cfg.consumerGroup == "" {
		errs = append(errs, errNoConsumerGroup)
	}
	if err := checkEventPublisher(os.Getenv("EVENT_PUBLISHER")); err != nil {
		errs = append(errs, err)
	}
	if err := errors.Join(errs...); err != nil {
		return bootConfig{}, fmt.Errorf("invalid configuration: %w", err)
	}
	return cfg, nil
}
