package architecture

import (
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"testing"
)

// TestNoSiblingContextLiveCalls makes the "no sibling REST/MCP calls" rule
// of labor-performance executable. The rule is prose today:
//
//	AGENTS.md "Non-negotiables" #1: "It NEVER calls ANY sibling context over
//	REST or MCP — no live lookups, ever; cross-context data must be local/
//	declarative." Also ADR 0003: it "MUST NOT call either sibling context
//	synchronously over HTTP, ever".
//
// This test parses (go/parser + go/ast) every non-test Go file under
// internal/ and cmd/ (cmd/ is where the composition root reads env vars,
// so a sibling base URL would surface there first) and fails on any of:
//
//  1. http-client      net/http used as a CLIENT: http.Client, http.Get/Post/
//     PostForm/Head, http.NewRequest[WithContext], http.DefaultClient,
//     http.DefaultTransport, http.Transport. SERVER use (Handler, Server,
//     ServeMux, ResponseWriter, Header, status consts ...) is not matched.
//  2. mcp-client       the modelcontextprotocol/go-sdk CLIENT surface
//     (NewClient, ClientSession, StreamableClientTransport, ...). Building an
//     MCP SERVER with the same SDK is allowed.
//  3. banned-import    gRPC and the a2a-go client packages (also banned by
//     the rule: "no REST, no MCP, no gRPC") and third-party HTTP client libs.
//  4. env-url          a string literal naming an env var that ends in _URL
//     (_BASE_URL, _A2A_URL ...): the shape a sibling-context base URL takes.
//     Own-infrastructure names are allowlisted in ownInfraEnvNames.
//  5. gateway-file     a gateway.go under internal/adapters/outbound, the
//     fleet's name for a sibling-context client adapter.
//
// AST matching (not substring) means comments and doc strings that merely
// MENTION http.Client are not findings. Legitimate exceptions are exact
// file+rule(+subject) entries in siblingCallAllowlist and must cite an ADR;
// a stale entry (no longer matched) fails the test too, so exceptions
// cannot outlive their reason. TestSiblingCallDetector proves the detector
// bites using in-memory snippets.

const (
	ruleHTTPClient   = "http-client"
	ruleMCPClient    = "mcp-client"
	ruleBannedImport = "banned-import"
	ruleEnvURL       = "env-url"
	ruleGatewayFile  = "gateway-file"

	mcpSDKImport = "github.com/modelcontextprotocol/go-sdk/mcp"
)

// siblingCallAllowlist: "<file>|<rule>|<subject>" (subject "*" = any) ->
// reason, which MUST cite the ADR that documents the exception.
var siblingCallAllowlist = map[string]string{
	// No exceptions: labor-performance has no same-context HTTP/MCP client.
}

// ownInfraEnvNames are *_URL env vars that point at this service's OWN
// infrastructure, never at a sibling context.
var ownInfraEnvNames = map[string]string{
	"DATABASE_URL":            "own Postgres (OLTP)",
	"MIGRATIONS_DATABASE_URL": "own Postgres, direct connection for migrations",
	"ANALYTICS_DATABASE_URL":  "own analytical Postgres",
	"A2A_AGENT_URL":           "this agent's OWN public card URL (ADR 0036), not a peer",
}

var (
	envURLNameRE = regexp.MustCompile(`^[A-Z][A-Z0-9]*(_[A-Z0-9]+)*_URL$`)

	httpClientSelectors = setOf("Client", "Get", "Post", "PostForm", "Head", "NewRequest",
		"NewRequestWithContext", "DefaultClient", "DefaultTransport", "Transport")
	mcpClientSelectors = setOf("NewClient", "Client", "ClientOptions", "ClientSession",
		"ClientSessionOptions", "StreamableClientTransport", "SSEClientTransport", "CommandTransport")
	bannedImportPrefixes = []string{
		"google.golang.org/grpc",
		"github.com/a2aproject/a2a-go/v2/a2aclient",
		"github.com/a2aproject/a2a-go/v2/a2agrpc",
		"github.com/go-resty/resty",
		"github.com/hashicorp/go-retryablehttp",
		"github.com/imroc/req",
	}
)

func setOf(vals ...string) map[string]bool {
	m := make(map[string]bool, len(vals))
	for _, v := range vals {
		m[v] = true
	}
	return m
}

type siblingFinding struct {
	file    string // module-root-relative, slash separated
	line    int
	rule    string
	subject string
}

func (f siblingFinding) String() string {
	return fmt.Sprintf("%s:%d: [%s] %s", f.file, f.line, f.rule, f.subject)
}

// importAliases maps each file-local import name to its import path.
func importAliases(f *ast.File) map[string]string {
	out := map[string]string{}
	for _, imp := range f.Imports {
		p, err := strconv.Unquote(imp.Path.Value)
		if err != nil {
			continue
		}
		name := filepath.Base(p)
		if imp.Name != nil {
			name = imp.Name.Name
		}
		out[name] = p
	}
	return out
}

func importFindings(fset *token.FileSet, f *ast.File, rel string) []siblingFinding {
	var out []siblingFinding
	for _, imp := range f.Imports {
		p, err := strconv.Unquote(imp.Path.Value)
		if err != nil {
			continue
		}
		line := fset.Position(imp.Pos()).Line
		if imp.Name != nil && imp.Name.Name == "." && (p == "net/http" || p == mcpSDKImport) {
			out = append(out, siblingFinding{rel, line, ruleBannedImport, "dot-import of " + p + " hides client use"})
		}
		for _, banned := range bannedImportPrefixes {
			if p == banned || strings.HasPrefix(p, banned+"/") {
				out = append(out, siblingFinding{rel, line, ruleBannedImport, p})
			}
		}
	}
	return out
}

func selectorFindings(fset *token.FileSet, f *ast.File, rel string) []siblingFinding {
	aliases := importAliases(f)
	var out []siblingFinding
	ast.Inspect(f, func(n ast.Node) bool {
		sel, ok := n.(*ast.SelectorExpr)
		if !ok {
			return true
		}
		id, ok := sel.X.(*ast.Ident)
		if !ok {
			return true
		}
		line := fset.Position(sel.Pos()).Line
		switch aliases[id.Name] {
		case "net/http":
			if httpClientSelectors[sel.Sel.Name] {
				out = append(out, siblingFinding{rel, line, ruleHTTPClient, "http." + sel.Sel.Name})
			}
		case mcpSDKImport:
			if mcpClientSelectors[sel.Sel.Name] {
				out = append(out, siblingFinding{rel, line, ruleMCPClient, "mcp." + sel.Sel.Name})
			}
		}
		return true
	})
	return out
}

func envLiteralFindings(fset *token.FileSet, f *ast.File, rel string) []siblingFinding {
	var out []siblingFinding
	ast.Inspect(f, func(n ast.Node) bool {
		lit, ok := n.(*ast.BasicLit)
		if !ok || lit.Kind != token.STRING {
			return true
		}
		s, err := strconv.Unquote(lit.Value)
		if err != nil || !envURLNameRE.MatchString(s) {
			return true
		}
		if _, own := ownInfraEnvNames[s]; !own {
			out = append(out, siblingFinding{rel, fset.Position(lit.Pos()).Line, ruleEnvURL, s})
		}
		return true
	})
	return out
}

func gatewayFileFindings(rel string) []siblingFinding {
	base := filepath.Base(rel)
	if strings.HasPrefix(rel, "internal/adapters/outbound/") && strings.HasSuffix(base, "gateway.go") {
		return []siblingFinding{{rel, 1, ruleGatewayFile, base}}
	}
	return nil
}

// scanSiblingCalls runs every detector over one source file.
func scanSiblingCalls(rel string, src []byte) ([]siblingFinding, error) {
	fset := token.NewFileSet()
	f, err := parser.ParseFile(fset, rel, src, parser.SkipObjectResolution)
	if err != nil {
		return nil, err
	}
	out := gatewayFileFindings(rel)
	out = append(out, importFindings(fset, f, rel)...)
	out = append(out, selectorFindings(fset, f, rel)...)
	return append(out, envLiteralFindings(fset, f, rel)...), nil
}

// applySiblingAllowlist drops allowlisted findings and returns the allowlist
// keys that matched nothing (stale exceptions).
func applySiblingAllowlist(found []siblingFinding, allow map[string]string) (violations []siblingFinding, stale []string) {
	used := map[string]bool{}
	for _, f := range found {
		exact := f.file + "|" + f.rule + "|" + f.subject
		wild := f.file + "|" + f.rule + "|*"
		switch {
		case allow[exact] != "":
			used[exact] = true
		case allow[wild] != "":
			used[wild] = true
		default:
			violations = append(violations, f)
		}
	}
	for k := range allow {
		if !used[k] {
			stale = append(stale, k)
		}
	}
	sort.Strings(stale)
	return violations, stale
}

var siblingRuleFix = map[string]string{
	ruleHTTPClient:   "do not call another context over HTTP; keep a declarative local copy fed by that context's events (or a local config/seed) and read that",
	ruleMCPClient:    "do not call another context's MCP server; keep a declarative local copy fed by events",
	ruleBannedImport: "remove the client import; cross-context data arrives only as events this service is allowed to consume",
	ruleEnvURL:       "a sibling base URL must not be configured at all; if this URL is this context's OWN service, add it to ownInfraEnvNames/siblingCallAllowlist with the ADR that documents it",
	ruleGatewayFile:  "an outbound gateway is a sibling-context client by fleet convention; model the data as a local declarative copy fed by events",
}

func siblingViolationMessage(v []siblingFinding) string {
	var b strings.Builder
	b.WriteString("ARCHITECTURE VIOLATION (sibling-context live call)\n")
	b.WriteString("  RULE:  labor-performance NEVER calls ANY sibling context over REST or MCP (AGENTS.md Non-negotiables #1, ADR 0003): no live lookups, ever; cross-context data must be local/declarative.\n")
	for _, f := range v {
		fmt.Fprintf(&b, "  FOUND: %s\n         FIX: %s\n", f, siblingRuleFix[f.rule])
	}
	b.WriteString("  NOTE:  a genuine same-context exception needs an ADR first, then an exact siblingCallAllowlist entry citing it.\n")
	return b.String()
}

func TestNoSiblingContextLiveCalls(t *testing.T) {
	var found []siblingFinding
	for _, root := range []string{"../../internal", "../../cmd"} {
		for _, path := range goFilesUnder(t, root, false) {
			src, err := os.ReadFile(path)
			if err != nil {
				t.Fatalf("read %s: %v", path, err)
			}
			rel := strings.TrimPrefix(filepath.ToSlash(path), "../../")
			fs, err := scanSiblingCalls(rel, src)
			if err != nil {
				t.Fatalf("parse %s: %v", rel, err)
			}
			found = append(found, fs...)
		}
	}
	violations, stale := applySiblingAllowlist(found, siblingCallAllowlist)
	if len(violations) > 0 {
		t.Error(siblingViolationMessage(violations))
	}
	for _, k := range stale {
		t.Errorf("stale siblingCallAllowlist entry %q matches nothing: remove it (an exception must not outlive its reason)", k)
	}
}

func TestSiblingCallDetector(t *testing.T) {
	const gw = "internal/adapters/outbound/foo/gateway.go"
	const adapter = "internal/adapters/inbound/x/x.go"
	cases := []struct {
		name string
		rel  string
		src  string
		want []string
	}{
		{"http.Client literal", adapter, "package x\nimport \"net/http\"\nvar c = &http.Client{}\n", []string{ruleHTTPClient}},
		{"http.Get call", adapter, "package x\nimport \"net/http\"\nfunc f() { _, _ = http.Get(\"u\") }\n", []string{ruleHTTPClient}},
		{"http.DefaultClient", adapter, "package x\nimport \"net/http\"\nvar c = http.DefaultClient\n", []string{ruleHTTPClient}},
		{"aliased net/http NewRequest", adapter, "package x\nimport nh \"net/http\"\nfunc f() { _, _ = nh.NewRequest(\"GET\", \"u\", nil) }\n", []string{ruleHTTPClient}},
		{"dot-import net/http", adapter, "package x\nimport . \"net/http\"\n", []string{ruleBannedImport}},
		{"server-only net/http is clean", adapter, "package x\nimport \"net/http\"\n" +
			"var _ http.Handler\nvar _ = &http.Server{Handler: http.NewServeMux()}\n" +
			"func h(w http.ResponseWriter, r *http.Request) { w.WriteHeader(http.StatusOK); _ = http.Header{} }\n", nil},
		{"comment mentioning http.Client is clean", adapter, "package x\n// never build an http.Client here\nvar s = \"see http.Client docs\"\n", nil},
		{"mcp client", adapter, "package x\nimport \"github.com/modelcontextprotocol/go-sdk/mcp\"\n" +
			"var c = mcp.NewClient(nil, nil)\nvar _ = &mcp.StreamableClientTransport{}\n", []string{ruleMCPClient, ruleMCPClient}},
		{"mcp server is clean", adapter, "package x\nimport \"github.com/modelcontextprotocol/go-sdk/mcp\"\nvar s = mcp.NewServer(nil, nil)\n", nil},
		{"a2a client import", adapter, "package x\nimport _ \"github.com/a2aproject/a2a-go/v2/a2aclient\"\n", []string{ruleBannedImport}},
		{"grpc import", adapter, "package x\nimport _ \"google.golang.org/grpc\"\n", []string{ruleBannedImport}},
		{"sibling base url env", "cmd/x/main.go", "package main\nimport \"os\"\nvar u = os.Getenv(\"INVENTORY_BASE_URL\")\n", []string{ruleEnvURL}},
		{"sibling a2a url env via const", "cmd/x/main.go", "package main\nconst k = \"PEER_A2A_URL\"\n", []string{ruleEnvURL}},
		{"own database url env is clean", "cmd/x/main.go", "package main\nimport \"os\"\nvar u = os.Getenv(\"DATABASE_URL\")\n", nil},
		{"non-url env is clean", "cmd/x/main.go", "package main\nimport \"os\"\nvar u = os.Getenv(\"KAFKA_BROKERS\")\n", nil},
		{"outbound gateway.go", gw, "package foo\n", []string{ruleGatewayFile}},
		{"gateway.go outside outbound is clean", adapter[:len(adapter)-4] + "gateway.go", "package x\n", nil},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			fs, err := scanSiblingCalls(tc.rel, []byte(tc.src))
			if err != nil {
				t.Fatalf("scan: %v", err)
			}
			var got []string
			for _, f := range fs {
				got = append(got, f.rule)
			}
			if strings.Join(got, ",") != strings.Join(tc.want, ",") {
				t.Fatalf("rules = %v, want %v (findings: %v)", got, tc.want, fs)
			}
		})
	}
}

func TestSiblingCallAllowlistLogic(t *testing.T) {
	found := []siblingFinding{
		{"a.go", 3, ruleHTTPClient, "http.Client"},
		{"b.go", 4, ruleHTTPClient, "http.Client"},
	}
	allow := map[string]string{
		"a.go|http-client|*":    "ADR test",
		"gone.go|http-client|*": "ADR test",
	}
	v, stale := applySiblingAllowlist(found, allow)
	if len(v) != 1 || v[0].file != "b.go" {
		t.Fatalf("violations = %v, want only b.go", v)
	}
	if len(stale) != 1 || stale[0] != "gone.go|http-client|*" {
		t.Fatalf("stale = %v, want the unmatched entry", stale)
	}
}
