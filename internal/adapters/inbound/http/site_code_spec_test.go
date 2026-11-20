package http_test

import (
	"os"
	"regexp"
	"strings"
	"testing"

	"gopkg.in/yaml.v3"

	"github.com/claudioed/labor-performance/internal/domain/shared"
)

// The SiteCode value schemas must admit EXACTLY what the domain constructor
// shared.NewSiteCode (internal/domain/shared/shared.go:150) admits: 1 to
// shared.MaxSiteCodeLength code points, valid UTF-8, and no rune for which
// unicode.IsSpace is true (Go's White_Space set). The constructor is the
// single source of truth; the spec encodes it for every caller that reads
// apis/openapi.yaml — above all the Schemathesis `contract` CI job, which
// generates values from the schema and treats a 400 for a schema-compliant
// value as a failure ("API rejected schema-compliant request").
//
// Why this test exists: the pattern used to read `^\S+$`, which does NOT
// spell the constructor's rule. \S is engine-dependent — ECMA-262 (JSON
// Schema's pattern language) excludes U+00A0 but admits U+0085, Python's re
// excludes both but also excludes U+001C-U+001F that Go accepts, and
// Schemathesis 4.28's generation admitted U+000B/U+0085/U+00A0 outright — so
// the contract job generated schema-valid site codes the domain refused with
// 400 invalid-site-code, at random: CI run 44738035745 (PR #282's first head
// run) failed on GET /standards/PACK?siteCode=%5E%C2%A0%C2%9Aq%3F%12O — the
// %C2%A0 is a U+00A0 NO-BREAK SPACE. A local probe with the pinned
// Schemathesis 4.28.0 and CI's exact flags reproduced it 7/10 seeds against
// the old pattern and 0/10 against the explicit White_Space class.

type specStringSchema struct {
	Pattern   string `yaml:"pattern"`
	MinLength *int   `yaml:"minLength"`
	MaxLength *int   `yaml:"maxLength"`
}

type openapiDoc struct {
	Components struct {
		Schemas map[string]specStringSchema `yaml:"schemas"`
	} `yaml:"components"`
}

type asyncapiPayloadSchema struct {
	Properties map[string]specStringSchema `yaml:"properties"`
}

type asyncapiDoc struct {
	Components struct {
		Schemas map[string]asyncapiPayloadSchema `yaml:"schemas"`
	} `yaml:"components"`
}

// siteCodeSchemas returns the wire schemas constraining a SiteCode value:
// the OpenAPI SiteCode component (siteCode body field and query parameter —
// the surface the Schemathesis `contract` job generates from, and the one
// this repo owns end to end) and the AsyncAPI site_code payload fields of
// the two standard events (length bounds only — their `^\S+$` pattern has
// the same drift, but fixing it is a coordinated fleet change: the producer
// breaking gate (scripts/harness/asyncapi_breaking.py) classifies any
// pattern change on an owned payload field as breaking, and
// warehouse-planning pins apis/asyncapi.yaml byte-for-byte. See the ADR
// 0038 addendum and the PR's open points).
func siteCodeSchemas(t *testing.T) (openapi specStringSchema, asyncapi map[string]specStringSchema) {
	t.Helper()

	var ol openapiDoc
	o, err := os.ReadFile("../../../../apis/openapi.yaml")
	if err != nil {
		t.Fatalf("read apis/openapi.yaml: %v", err)
	}
	if err := yaml.Unmarshal(o, &ol); err != nil {
		t.Fatalf("parse apis/openapi.yaml: %v", err)
	}
	site, ok := ol.Components.Schemas["SiteCode"]
	if !ok || site.Pattern == "" {
		t.Fatal("apis/openapi.yaml: components.schemas.SiteCode.pattern is missing")
	}

	var al asyncapiDoc
	a, err := os.ReadFile("../../../../apis/asyncapi.yaml")
	if err != nil {
		t.Fatalf("read apis/asyncapi.yaml: %v", err)
	}
	if err := yaml.Unmarshal(a, &al); err != nil {
		t.Fatalf("parse apis/asyncapi.yaml: %v", err)
	}
	events := make(map[string]specStringSchema)
	for _, event := range []string{"LaborStandardDefinedData", "LaborStandardRevisedData"} {
		payload, ok := al.Components.Schemas[event]
		if !ok {
			t.Fatalf("apis/asyncapi.yaml: components.schemas.%s is missing", event)
		}
		field, ok := payload.Properties["site_code"]
		if !ok || field.Pattern == "" {
			t.Fatalf("apis/asyncapi.yaml: %s.properties.site_code.pattern is missing", event)
		}
		events[event+".properties.site_code"] = field
	}
	return site, events
}

// ecmaEscape translates a JSON Schema (ECMA-262) pattern to Go's RE2
// dialect. Only the escape SPELLING is rewritten (\uXXXX -> \x{XXXX}); the
// code points and semantics are unchanged, so what is compiled is what a
// JSON Schema validator or Schemathesis reads.
var ecmaEscape = regexp.MustCompile(`\\u([0-9a-fA-F]{4})`)

func compileSpecPattern(t *testing.T, pattern string) *regexp.Regexp {
	t.Helper()
	re, err := regexp.Compile(ecmaEscape.ReplaceAllString(pattern, `\x{$1}`))
	if err != nil {
		t.Fatalf("pattern %q does not compile: %v", pattern, err)
	}
	return re
}

// TestSiteCodeSchemasAdmitExactlyWhatNewSiteCodeAccepts pins the schema to
// the constructor in BOTH directions: nothing the domain refuses may be
// schema-compliant (that is the flaky-contract-job direction), and nothing
// the domain accepts may be schema-invalid.
func TestSiteCodeSchemasAdmitExactlyWhatNewSiteCodeAccepts(t *testing.T) {
	openapi, _ := siteCodeSchemas(t)
	patterns := map[string]specStringSchema{
		"apis/openapi.yaml components.schemas.SiteCode": openapi,
	}

	// Every rune of Go's White_Space set (unicode.IsSpace, the constructor's
	// refusal at shared.go:154-158), embedded the way a generator would embed
	// it, plus the exact value CI run 44738035745 generated. The length
	// refusal (shared.go:151) is pinned by the bounds test below: the schema
	// spells it minLength/maxLength, not in the pattern.
	refused := map[string]string{
		"tab":                             "SITE\tGRU",
		"line feed":                       "SITE\nGRU",
		"vertical tab":                    "SITE\vGRU",
		"form feed":                       "SITE\fGRU",
		"carriage return":                 "SITE\rGRU",
		"inner space":                     "SITE GRU",
		"leading space":                   " SITE",
		"trailing space":                  "SITE ",
		"only a space":                    " ",
		"NEL U+0085 (ECMA \\S admits it)": "SITE\u0085GRU",
		"bare NEL":                        "\u0085",
		"NBSP U+00A0 (the CI flake)":      "SITE\u00a0GRU",
		"bare NBSP":                       "\u00a0",
		"ogham space mark U+1680":         "SITE\u1680GRU",
		"en quad U+2000 (range start)":    "SITE\u2000GRU",
		"figure space U+2007 (mid-range)": "SITE\u2007GRU",
		"hair space U+200A (range end)":   "SITE\u200aGRU",
		"line separator U+2028":           "SITE\u2028GRU",
		"paragraph separator U+2029":      "SITE\u2029GRU",
		"narrow NBSP U+202F":              "SITE\u202fGRU",
		"medium math space U+205F":        "SITE\u205fGRU",
		"ideographic space U+3000":        "SITE\u3000GRU",
		"the CI-generated value":          "^\u00a0\u009aq?\x12O",
	}
	for name, value := range refused {
		if _, err := shared.NewSiteCode(value); err == nil {
			t.Errorf("table drift: NewSiteCode(%q) [%s] is accepted; the constructor is the source of truth — fix the test table", value, name)
			continue
		}
		for where, schema := range patterns {
			if compileSpecPattern(t, schema.Pattern).MatchString(value) {
				t.Errorf("%s: pattern %q admits %q [%s], but shared.NewSiteCode refuses it — Schemathesis will generate it as schema-compliant and the contract job will fail on the 400 (run 44738035745)", where, schema.Pattern, value, name)
			}
		}
	}

	// The other direction: values the constructor ACCEPTS. U+001C-U+001F
	// (Python re's \\s calls them space; Go's White_Space does not), U+FEFF
	// (ECMA's \\s calls it space; Go does not), currency symbols and
	// zero-width characters are all legitimate site codes — the schema must
	// not refuse what the domain allows.
	accepted := map[string]string{
		"the spec example":                   "SITE-GRU-01",
		"32 characters":                      strings.Repeat("S", shared.MaxSiteCodeLength),
		"32 multi-byte characters":           strings.Repeat("é", shared.MaxSiteCodeLength),
		"file separator U+001C (Go accepts)": "SITE\u001cGRU",
		"BOM U+FEFF (Go accepts)":            "SITE\ufeff",
		"currency symbols":                   "$ITE-€₹",
		"zero-width space (not White_Space)": "SITE\u200bGRU",
		"DEL and C1 controls":                "A\u007fB\u009b",
	}
	for name, value := range accepted {
		if _, err := shared.NewSiteCode(value); err != nil {
			t.Errorf("table drift: NewSiteCode(%q) [%s] is refused; the constructor is the source of truth — fix the test table", value, name)
			continue
		}
		for where, schema := range patterns {
			if !compileSpecPattern(t, schema.Pattern).MatchString(value) {
				t.Errorf("%s: pattern %q refuses %q [%s], but shared.NewSiteCode accepts it — the schema must not be stricter than the domain either", where, schema.Pattern, value, name)
			}
		}
	}
}

// TestSiteCodeSchemaLengthBoundsMatchConstructor pins the length bounds to
// the constructor's: a present siteCode is 1..MaxSiteCodeLength code points
// (shared.go:151; the empty string means "no site code" and travels as an
// absent optional field, not as "").
func TestSiteCodeSchemaLengthBoundsMatchConstructor(t *testing.T) {
	openapi, asyncapi := siteCodeSchemas(t)
	for where, schema := range merge(map[string]specStringSchema{
		"apis/openapi.yaml components.schemas.SiteCode": openapi,
	}, asyncapi) {
		if schema.MinLength == nil || *schema.MinLength != 1 {
			t.Errorf("%s: minLength = %v, want 1", where, schema.MinLength)
		}
		if schema.MaxLength == nil || *schema.MaxLength != shared.MaxSiteCodeLength {
			t.Errorf("%s: maxLength = %v, want %d (shared.MaxSiteCodeLength)", where, schema.MaxLength, shared.MaxSiteCodeLength)
		}
	}
}

func merge(a, b map[string]specStringSchema) map[string]specStringSchema {
	out := make(map[string]specStringSchema, len(a)+len(b))
	for k, v := range a {
		out[k] = v
	}
	for k, v := range b {
		out[k] = v
	}
	return out
}
