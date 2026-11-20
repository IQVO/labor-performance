package http

import (
	"net/http"
	"strings"

	"github.com/go-chi/chi/v5"
)

// The problems chi's router answers with when no handler matches are
// problemResourceNotFound and problemMethodNotAllowed (problems.go). Without
// them, chi falls back to net/http's text/plain "404 page not found" and a
// bare 405, neither of which is an RFC 7807 problem. The slugs match the
// sibling contexts (resource-not-found is also what rejectNUL answers for a
// NUL in the path; method-not-allowed is the fleet-wide 405 slug).

// handleRouteNotFound answers a request whose path matches no route.
func handleRouteNotFound(w http.ResponseWriter, r *http.Request) {
	writeProblem(w, http.StatusNotFound, problemResourceNotFound, "no resource matches the requested path", r.URL.Path)
}

// probeMethods are the methods chi can route; allowedMethods tries each.
var probeMethods = []string{
	http.MethodGet, http.MethodHead, http.MethodPost, http.MethodPut, http.MethodPatch,
	http.MethodDelete, http.MethodOptions, http.MethodConnect, http.MethodTrace,
}

// allowedMethods lists, in probeMethods order, the methods routes has a
// handler for on path, as an Allow header value ("" when none).
func allowedMethods(routes chi.Routes, path string) string {
	var allowed []string
	for _, m := range probeMethods {
		if routes.Match(chi.NewRouteContext(), m, path) {
			allowed = append(allowed, m)
		}
	}
	return strings.Join(allowed, ", ")
}

// routedPath is the path the router matched on: chi strips a mount prefix into
// RoutePath for sub-routers.
func routedPath(r *http.Request) string {
	if rctx := chi.RouteContext(r.Context()); rctx != nil && rctx.RoutePath != "" {
		return rctx.RoutePath
	}
	return r.URL.Path
}

// methodNotAllowedFor answers a request whose path exists but not for the
// request's method, as an RFC 7807 problem WITH the Allow header RFC 9110
// section 15.5.6 requires. chi sets Allow only in its DEFAULT 405 handler: once
// a custom handler is registered it is called as-is, so the list is derived
// from the router itself.
func methodNotAllowedFor(routes chi.Routes) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if allow := allowedMethods(routes, routedPath(r)); allow != "" {
			w.Header().Set("Allow", allow)
		}
		writeProblem(w, http.StatusMethodNotAllowed, problemMethodNotAllowed, "the method is not supported on this path", r.URL.Path)
	}
}
