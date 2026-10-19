package main

import (
	"errors"
	"fmt"
	"os"
	"strings"
)

// errNoBrokers is the boot error of a missing KAFKA_BROKERS: the projector's
// only input is the analytics topic on real Kafka, so there is no localhost
// default and no degraded mode (ADR 0034).
var errNoBrokers = errors.New("KAFKA_BROKERS is required: this context runs on real Kafka")

// parseBrokers splits KAFKA_BROKERS (comma separated), trimming blanks. It
// returns nil when nothing is configured.
func parseBrokers(raw string) []string {
	var brokers []string
	for _, b := range strings.Split(raw, ",") {
		if b = strings.TrimSpace(b); b != "" {
			brokers = append(brokers, b)
		}
	}
	return brokers
}

// bootConfig is the validated mandatory configuration of cmd/labor-projector.
type bootConfig struct {
	analyticsURL string
	kafkaBrokers []string
}

// loadBootConfig reads and validates the mandatory environment. Every problem
// is reported at once, each naming its variable, before anything is opened.
func loadBootConfig() (bootConfig, error) {
	cfg := bootConfig{
		analyticsURL: os.Getenv("ANALYTICS_DATABASE_URL"),
		kafkaBrokers: parseBrokers(os.Getenv("KAFKA_BROKERS")),
	}
	var errs []error
	if cfg.analyticsURL == "" {
		errs = append(errs, errMissingAnalyticsURL)
	}
	if len(cfg.kafkaBrokers) == 0 {
		errs = append(errs, errNoBrokers)
	}
	if err := errors.Join(errs...); err != nil {
		return bootConfig{}, fmt.Errorf("invalid configuration: %w", err)
	}
	return cfg, nil
}
