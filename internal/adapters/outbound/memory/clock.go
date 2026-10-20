// Package memory provides in-memory implementations of the outbound
// ports. They are TEST DOUBLES for the unit and BDD tests: no cmd/**
// binary imports this package (internal/architecture
// TestRuntimeBinaryHasNoInMemoryAdapters); the shipped binaries run on real
// Postgres and real Kafka only (ADR 0034). The production wall clock lives
// in internal/adapters/outbound/system.
package memory

import "time"

// FixedClock implements ports.Clock with a fixed, settable time, so
// timestamp assertions in tests are exact rather than tolerance-based.
type FixedClock struct {
	At time.Time
}

func (c FixedClock) Now() time.Time { return c.At }
