package http_test

import (
	"net/http"
	"net/url"
	"strings"
	"testing"
)

// ADR 0038 decision 2 on the REST surface: POST /standards accepts an
// optional siteCode, the Standard body carries it only when set, and
// GET /standards/{taskType}?siteCode= reads exactly that site's standard.

func TestPostStandards_SiteCodeIsEchoed(t *testing.T) {
	e := newTestEnv(t, now)
	rec := e.do(t, http.MethodPost, "/standards", `{"taskType":"PICK","expectedSeconds":40,"siteCode":"SITE-GRU-01"}`)
	if rec.Code != http.StatusCreated {
		t.Fatalf("status = %d (%s)", rec.Code, rec.Body.String())
	}
	if m := decodeMap(t, rec.Body.Bytes()); m["siteCode"] != "SITE-GRU-01" || m["expectedSeconds"] != float64(40) {
		t.Fatalf("body = %v", m)
	}
}

// TestPostStandards_NoSiteCode_BodyIsUnchanged: a site-less standard's body
// has no siteCode key at all, so it is byte-for-byte the pre-ADR-0038 body.
func TestPostStandards_NoSiteCode_BodyIsUnchanged(t *testing.T) {
	e := newTestEnv(t, now)
	for _, body := range []string{
		`{"taskType":"PICK","expectedSeconds":45}`,
		`{"taskType":"PACK","expectedSeconds":45,"siteCode":""}`,
	} {
		rec := e.do(t, http.MethodPost, "/standards", body)
		if rec.Code != http.StatusCreated {
			t.Fatalf("%s: status = %d (%s)", body, rec.Code, rec.Body.String())
		}
		if strings.Contains(rec.Body.String(), "siteCode") {
			t.Fatalf("%s: siteCode must be omitted for a site-less standard: %s", body, rec.Body.String())
		}
	}
}

func TestPostStandards_SiteCodeRejections(t *testing.T) {
	for name, site := range map[string]string{
		"33 characters": strings.Repeat("S", 33),
		"inner space":   "SITE GRU",
		"leading space": " SITE",
		"tab":           "SITE\\tGRU",
		"only a space":  " ",
	} {
		t.Run(name, func(t *testing.T) {
			e := newTestEnv(t, now)
			p := assertProblem(t, e.do(t, http.MethodPost, "/standards", `{"taskType":"PICK","expectedSeconds":30,"siteCode":"`+site+`"}`), http.StatusBadRequest)
			if !strings.HasSuffix(p.Type, "/invalid-site-code") {
				t.Fatalf("problem.type = %q, want suffix invalid-site-code", p.Type)
			}
			if got := e.do(t, http.MethodGet, "/standards/PICK", ""); got.Code != http.StatusNotFound {
				t.Fatalf("a rejected definition must store nothing, GET = %d", got.Code)
			}
		})
	}
	// Exactly 32 characters is the longest accepted code.
	e := newTestEnv(t, now)
	if rec := e.do(t, http.MethodPost, "/standards", `{"taskType":"PICK","expectedSeconds":30,"siteCode":"`+strings.Repeat("S", 32)+`"}`); rec.Code != http.StatusCreated {
		t.Fatalf("32 characters: status = %d (%s)", rec.Code, rec.Body.String())
	}
}

func TestGetStandard_SiteCodeQueryReadsExactlyThatSite(t *testing.T) {
	e := newTestEnv(t, now)
	for _, body := range []string{
		`{"taskType":"PICK","expectedSeconds":45}`,
		`{"taskType":"PICK","expectedSeconds":40,"siteCode":"SITE-GRU-01"}`,
		`{"taskType":"PICK","expectedSeconds":28,"handlingUnit":"CASE","siteCode":"SITE-GRU-01"}`,
	} {
		if rec := e.do(t, http.MethodPost, "/standards", body); rec.Code != http.StatusCreated {
			t.Fatalf("setup %s: %d %s", body, rec.Code, rec.Body.String())
		}
	}
	get := func(path string) map[string]any {
		t.Helper()
		rec := e.do(t, http.MethodGet, path, "")
		if rec.Code != http.StatusOK {
			t.Fatalf("GET %s = %d (%s)", path, rec.Code, rec.Body.String())
		}
		return decodeMap(t, rec.Body.Bytes())
	}
	// Without siteCode: the site-less standard, exactly as before.
	if m := get("/standards/PICK"); m["expectedSeconds"] != float64(45) || m["siteCode"] != nil {
		t.Fatalf("no query must stay the site-less standard: %v", m)
	}
	if m := get("/standards/PICK?siteCode="); m["expectedSeconds"] != float64(45) || m["siteCode"] != nil {
		t.Fatalf("an empty siteCode is no site: %v", m)
	}
	if m := get("/standards/PICK?siteCode=SITE-GRU-01"); m["expectedSeconds"] != float64(40) || m["siteCode"] != "SITE-GRU-01" {
		t.Fatalf("site standard: %v", m)
	}
	if m := get("/standards/PICK?siteCode=SITE-GRU-01&handlingUnit=CASE"); m["expectedSeconds"] != float64(28) || m["handlingUnit"] != "CASE" || m["siteCode"] != "SITE-GRU-01" {
		t.Fatalf("site CASE standard: %v", m)
	}
	// A site without its own standard is 404: never the site-less fallback.
	assertProblem(t, e.do(t, http.MethodGet, "/standards/PICK?siteCode=SITE-POA-02", ""), http.StatusNotFound)
	assertProblem(t, e.do(t, http.MethodGet, "/standards/PICK?siteCode=SITE-GRU-01&handlingUnit=EACH", ""), http.StatusNotFound)
	assertProblem(t, e.do(t, http.MethodGet, "/standards/PACK?siteCode=SITE-GRU-01", ""), http.StatusNotFound)
}

func TestGetStandard_SiteCodeQueryErrors(t *testing.T) {
	e := newTestEnv(t, now)
	for _, site := range []string{strings.Repeat("S", 33), "SITE GRU", " "} {
		p := assertProblem(t, e.do(t, http.MethodGet, "/standards/PICK?siteCode="+url.QueryEscape(site), ""), http.StatusBadRequest)
		if !strings.HasSuffix(p.Type, "/invalid-site-code") {
			t.Fatalf("siteCode %q: problem.type = %q", site, p.Type)
		}
	}
	// An invalid handlingUnit is still reported first.
	p := assertProblem(t, e.do(t, http.MethodGet, "/standards/PICK?handlingUnit=CARTON&siteCode=SITE-GRU-01", ""), http.StatusBadRequest)
	if !strings.HasSuffix(p.Type, "/unknown-handling-unit") {
		t.Fatalf("problem.type = %q", p.Type)
	}
}
