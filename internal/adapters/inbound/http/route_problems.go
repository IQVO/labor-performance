package http

import "net/http"

// The problems chi's router answers with when no handler matches. Without
// these, chi falls back to net/http's text/plain "404 page not found" and a
// bare 405, neither of which is an RFC 7807 problem. The slugs match the
// sibling contexts (resource-not-found is also what rejectNUL answers for a
// NUL in the path; method-not-allowed is the fleet-wide 405 slug).
var (
	problemResourceNotFound = problemInfo{"resource-not-found", "Resource not found"}
	problemMethodNotAllowed = problemInfo{"method-not-allowed", "Method not allowed"}
)

// handleRouteNotFound answers a request whose path matches no route.
func handleRouteNotFound(w http.ResponseWriter, r *http.Request) {
	writeProblem(w, http.StatusNotFound, problemResourceNotFound, "no resource matches the requested path", r.URL.Path)
}

// handleMethodNotAllowed answers a request whose path exists but not for the
// request's method.
func handleMethodNotAllowed(w http.ResponseWriter, r *http.Request) {
	writeProblem(w, http.StatusMethodNotAllowed, problemMethodNotAllowed, "the method is not supported on this path", r.URL.Path)
}
