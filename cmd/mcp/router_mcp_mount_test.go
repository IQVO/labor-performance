package main

import (
	"net/http"
	"net/http/httptest"
	"testing"
)

// TestRouterServesMcpMount pins the fleet's Kong host contract: the chart
// exposes this server at /api/mcp/labor-performance, rewritten to exactly
// /mcp. Until 2026-10-13 this binary mounted the MCP handler at / only and
// 404'd on /mcp — the one server in the fleet that did (found live by
// exposure-policy section 12). A POST that is NOT a valid JSON-RPC still
// proves the mount: the handler answers (here 400 from the SDK), not chi's
// plaintext "404 page not found".
func TestRouterServesMcpMount(t *testing.T) {
	handler := newRouter(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusBadRequest)
	}), "labor-performance-mcp-test")

	for _, path := range []string{"/", "/mcp"} {
		req := httptest.NewRequest(http.MethodPost, path, nil)
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, req)
		if rec.Code == http.StatusNotFound {
			t.Errorf("POST %s: got 404 — the MCP handler is not mounted at this path", path)
		}
	}

	// healthz stays open and unauthenticated for the probes.
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/healthz", nil))
	if rec.Code != http.StatusOK {
		t.Errorf("GET /healthz: got %d, want 200", rec.Code)
	}
}
