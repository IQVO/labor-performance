package main

import (
	"strings"
	"testing"
)

func setBootEnv(t *testing.T, overrides map[string]string) {
	t.Helper()
	env := map[string]string{
		"ANALYTICS_DATABASE_URL": "postgres://u:***@localhost:5432/labor_analytics?sslmode=disable",
		"KAFKA_BROKERS":          "kafka-1:9092,kafka-2:9092",
	}
	for k, v := range overrides {
		env[k] = v
	}
	for k, v := range env {
		t.Setenv(k, v)
	}
}

func TestLoadBootConfigRejects(t *testing.T) {
	tests := []struct {
		name      string
		overrides map[string]string
		wantNames []string
	}{
		{"no ANALYTICS_DATABASE_URL", map[string]string{"ANALYTICS_DATABASE_URL": ""}, []string{"ANALYTICS_DATABASE_URL"}},
		{"no KAFKA_BROKERS", map[string]string{"KAFKA_BROKERS": ""}, []string{"KAFKA_BROKERS"}},
		{"blank KAFKA_BROKERS entries", map[string]string{"KAFKA_BROKERS": " , "}, []string{"KAFKA_BROKERS"}},
		{"both missing are reported at once", map[string]string{"ANALYTICS_DATABASE_URL": "", "KAFKA_BROKERS": ""},
			[]string{"ANALYTICS_DATABASE_URL", "KAFKA_BROKERS"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			setBootEnv(t, tt.overrides)
			_, err := loadBootConfig()
			if err == nil {
				t.Fatal("loadBootConfig() = nil error, want a boot error")
			}
			for _, name := range tt.wantNames {
				if !strings.Contains(err.Error(), name) {
					t.Errorf("error %q does not mention %q", err, name)
				}
			}
		})
	}
}

func TestLoadBootConfigAccepts(t *testing.T) {
	setBootEnv(t, nil)
	cfg, err := loadBootConfig()
	if err != nil {
		t.Fatalf("loadBootConfig() = %v, want nil", err)
	}
	if got := strings.Join(cfg.kafkaBrokers, "|"); got != "kafka-1:9092|kafka-2:9092" {
		t.Errorf("kafkaBrokers = %q, want the two configured brokers", got)
	}
}

func TestParseBrokers(t *testing.T) {
	if got := parseBrokers(""); got != nil {
		t.Errorf("parseBrokers(\"\") = %v, want nil (no localhost default)", got)
	}
	if got := strings.Join(parseBrokers(" a:1 , ,b:2"), "|"); got != "a:1|b:2" {
		t.Errorf("parseBrokers = %q, want a:1|b:2", got)
	}
}
