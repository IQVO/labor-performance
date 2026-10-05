package mcp

import (
	"net/http"

	"github.com/modelcontextprotocol/go-sdk/mcp"
)

// NewServer builds the MCP server for this bounded context with every read
// tool and the scoped scorecard resource registered.
//
// labor-performance is a pure read-side reporter to the rest of the fleet —
// it consumes fulfillment-execution's TaskCompleted but exposes no write
// use case of its own for an agent to call, so no write tool is registered.
func NewServer(deps Deps) *mcp.Server {
	server := mcp.NewServer(
		&mcp.Implementation{Name: "labor-performance-mcp", Version: "1.0.0"},
		&mcp.ServerOptions{
			Instructions: "Read-only access to labor-performance: per-associate scorecards (trend, coaching flag), per-task-type fleet performance, active engineered labor standards, and per-task-type idleness/utilization. Start with the review_associate_performance prompt.",
		},
	)

	deps.registerTools(server)
	deps.registerResources(server)
	deps.registerPrompts(server)

	return server
}

// Handler returns the Streamable HTTP handler for the MCP server. REST/MCP
// identity was removed fleet-wide (see the ADR superseding ADR 0011); this
// server is unauthenticated, matching the rest of this context's inbound
// surfaces.
func Handler(server *mcp.Server) http.Handler {
	streamable := mcp.NewStreamableHTTPHandler(func(*http.Request) *mcp.Server { return server }, nil)
	return streamable
}
