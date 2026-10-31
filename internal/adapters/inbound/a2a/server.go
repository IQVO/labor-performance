// Package a2a is the inbound Agent2Agent adapter: it exposes this bounded
// context to the fleet's agent ecosystem as a third driving adapter over the
// same read use cases the HTTP and MCP adapters use. It is built on the
// official A2A Go SDK (a2aproject/a2a-go) and speaks JSON-RPC 2.0 over HTTP,
// with the Agent Card served at the well-known path.
//
// A2A is the DELEGATION channel between bounded contexts: an agent client
// sends a goal-shaped skill request as a structured message part and this
// agent answers from its own state through its own use cases. It is not a
// data-sync channel -- facts between contexts still travel exclusively as
// CloudEvents on Kafka (ADR-0021).
//
// labor-performance is a pure read-side reporter (AGENTS.md non-negotiables
// 1 and 4): recording a TaskPerformance is exclusively driven by the Kafka
// consumer and defining a standard by an operator over REST, and its MCP
// charter offers no write tool. This agent is therefore READ-ONLY: no write
// use case is wired and no skill changes state. It also adds no outbound
// client of any kind -- this context never calls a sibling over REST, MCP or
// A2A.
//
// Like the MCP adapter, this package depends inward on the application layer
// (use cases and ports) and the domain only -- never on an outbound adapter.
// The composition root (cmd/a2a) wires concrete repositories into the use
// cases. Domain structs never leak across the message boundary.
package a2a

import (
	"context"
	"iter"
	"net/http"

	"github.com/a2aproject/a2a-go/v2/a2a"
	"github.com/a2aproject/a2a-go/v2/a2asrv"

	"github.com/claudioed/labor-performance/internal/application/usecases"
)

// Config carries the deployment-specific identity of this agent: the absolute
// URL clients use to reach its JSON-RPC endpoint (advertised on the Agent
// Card) and the card version. It is supplied by the composition root from
// A2A_AGENT_URL; the card MUST advertise a URL the fleet's agent clients can
// actually dial (in-cluster: http://<release>-a2a.<ns>.svc.cluster.local:8093/).
type Config struct {
	AgentURL string
	Version  string
}

// Deps is everything the A2A skills need, injected by the composition root.
// It carries the SAME read use cases the HTTP and MCP adapters use; the
// adapter never constructs an outbound adapter itself, and no write use case
// is part of it by design (see the package doc).
type Deps struct {
	// GetAssociateScorecard is the read use case behind associate_scorecard.
	GetAssociateScorecard *usecases.GetAssociateScorecard
	// GetTaskTypePerformance, GetStandard and GetUtilization are the three
	// read use cases task_type_status composes.
	GetTaskTypePerformance *usecases.GetTaskTypePerformance
	GetStandard            *usecases.GetStandard
	GetUtilization         *usecases.GetUtilization
}

// Card builds this agent's Agent Card. The surface is two curated,
// goal-shaped read skills -- not one skill per endpoint, the same curation
// discipline the MCP charter applied. associate_scorecard answers "how is
// this associate doing"; task_type_status composes the three task-type reads
// a delegating agent needs to answer "how is this task type doing against its
// standard, and how busy". The card declares no security schemes on purpose:
// this surface is unauthenticated by the same fleet-wide decision as REST and
// MCP (ADR 0012); schemes are added here, not in a redesign, if that decision
// is ever reversed.
func Card(cfg Config) *a2a.AgentCard {
	return &a2a.AgentCard{
		Name:        "labor-performance",
		Description: "Labor Performance agent for the Warehouse Execution System (read-only): answers how an associate is performing (task count, mean efficiency vs. the engineered labor standard, per-task-type breakdown, trend and a coaching-flag visibility signal) and how a task type is performing across the whole floor (task count, mean efficiency, measured mean duration, the active engineered standard and idle-vs-busy utilization over a trailing window). Efficiency and utilization figures are null, never 0, when nothing was measurable. It never changes state: performance rows are recorded only from fulfillment-execution's TaskCompleted event, and standards are defined by an operator over REST.",
		Version:     cfg.Version,
		Provider: &a2a.AgentProvider{
			Org: "IQVO",
			URL: "https://github.com/IQVO/labor-performance",
		},
		SupportedInterfaces: []*a2a.AgentInterface{
			a2a.NewAgentInterface(cfg.AgentURL, a2a.TransportProtocolJSONRPC),
		},
		DefaultInputModes:  []string{"application/json", "text/plain"},
		DefaultOutputModes: []string{"application/json", "text/plain"},
		Capabilities:       a2a.AgentCapabilities{Streaming: false},
		Skills: []a2a.AgentSkill{
			{
				ID:          skillAssociateScorecard,
				Name:        "Performance scorecard for one associate",
				Description: "Read-only scorecard for one associate: task count, mean efficiency percent (null when no task was scorable), a per-task-type breakdown, a trend (IMPROVING, DECLINING, STABLE or INSUFFICIENT_DATA) and a coaching flag over their most recent tasks. The coaching flag is a visibility signal for a human, never a trigger to act autonomously. Answered as a 404 problem when no task performance is recorded for the associate.",
				Tags:        []string{"labor", "performance", "associate", "read"},
				InputModes:  []string{"application/json"},
				Examples: []string{
					`{"skill":"associate_scorecard","arguments":{"associateId":"assoc-1"}}`,
				},
			},
			{
				ID:          skillTaskTypeStatus,
				Name:        "Performance status of a task type",
				Description: "Read-only composed status of one task type (PICK, PACK or SLAM) across the whole floor: task count, mean efficiency percent and real measured mean duration (both null when nothing was measurable), the currently-active engineered labor standard (null when none is active, in which case efficiency cannot be scored) and idle-vs-busy utilization over a trailing window (windowSeconds, default 3600). utilizationPct is null when nothing was observed in the window; never treat null as 0%. Per-task-type utilization reports closed idle gaps only, so it can understate idleness by whatever gap is still open.",
				Tags:        []string{"labor", "performance", "task-type", "standard", "utilization", "read"},
				InputModes:  []string{"application/json"},
				Examples: []string{
					`{"skill":"task_type_status","arguments":{"taskType":"PICK"}}`,
					`{"skill":"task_type_status","arguments":{"taskType":"PACK","windowSeconds":7200}}`,
				},
			},
		},
	}
}

// NewExecutor returns the AgentExecutor that maps an incoming message to a
// skill and answers with a single agent Message (a message-only agent in A2A
// terms): both curated skills are request/response reads, so there is no
// long-running Task lifecycle to track. The response carries the caller's
// contextId (and taskId when the client referenced one) so a delegating saga
// can group its interactions.
func NewExecutor(deps Deps) a2asrv.AgentExecutor {
	return a2asrv.AgentExecutorFunc(func(ctx context.Context, ec *a2asrv.ExecutorContext) iter.Seq2[a2a.Event, error] {
		return func(yield func(a2a.Event, error) bool) {
			yield(answer(ctx, deps, ec), nil)
		}
	})
}

// Handler wires the executor into the transport-agnostic A2A request handler
// and wraps it in the JSON-RPC 2.0 HTTP handler -- the only transport this
// agent ships, mirroring the MCP adapter's "Streamable HTTP only" minimalism.
func Handler(executor a2asrv.AgentExecutor) http.Handler {
	return a2asrv.NewJSONRPCHandler(a2asrv.NewHandler(executor))
}
