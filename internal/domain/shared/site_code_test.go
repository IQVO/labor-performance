package shared_test

import (
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/claudioed/labor-performance/internal/domain/shared"
)

var eventTime = time.Date(2026, 11, 18, 9, 0, 0, 0, time.UTC)

// TestNewSiteCode_Boundaries pins the ADR 0038 site-code rule: "" is the
// site-less standard (every site); otherwise 1-32 characters (code points,
// not bytes) and no whitespace anywhere. Nothing else about the shape is
// assumed, so lowercase, punctuation and non-ASCII letters are all valid.
func TestNewSiteCode_Boundaries(t *testing.T) {
	tests := []struct {
		name    string
		in      string
		wantErr error
	}{
		{"empty is the site-less standard", "", nil},
		{"one character", "A", nil},
		{"typical code", "SITE-GRU-01", nil},
		{"lowercase and punctuation", "dc_east.2/b", nil},
		{"exactly 32 characters", strings.Repeat("S", shared.MaxSiteCodeLength), nil},
		{"32 multi-byte characters count as 32", strings.Repeat("é", shared.MaxSiteCodeLength), nil},
		{"33 characters", strings.Repeat("S", shared.MaxSiteCodeLength+1), shared.ErrInvalidSiteCode},
		{"33 multi-byte characters", strings.Repeat("é", shared.MaxSiteCodeLength+1), shared.ErrInvalidSiteCode},
		{"inner space", "SITE GRU", shared.ErrInvalidSiteCode},
		{"leading space", " SITE", shared.ErrInvalidSiteCode},
		{"trailing space", "SITE ", shared.ErrInvalidSiteCode},
		{"only a space", " ", shared.ErrInvalidSiteCode},
		{"tab", "SITE\tGRU", shared.ErrInvalidSiteCode},
		{"newline", "SITE\nGRU", shared.ErrInvalidSiteCode},
		{"no-break space", "SITE\u00a0GRU", shared.ErrInvalidSiteCode},
		{"ideographic space", "SITE\u3000GRU", shared.ErrInvalidSiteCode},
		{"invalid UTF-8", "SITE\xff", shared.ErrInvalidSiteCode},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, err := shared.NewSiteCode(tc.in)
			if !errors.Is(err, tc.wantErr) {
				t.Fatalf("NewSiteCode(%q) err = %v, want %v", tc.in, err, tc.wantErr)
			}
			if tc.wantErr == nil && got != shared.SiteCode(tc.in) {
				t.Fatalf("NewSiteCode(%q) = %q, want it unchanged", tc.in, got)
			}
			if tc.wantErr != nil && got != "" {
				t.Fatalf("NewSiteCode(%q) = %q on error, want \"\"", tc.in, got)
			}
		})
	}
}

// TestStandardEvents_WithSite: WithSite sets only the site, and the
// constructors keep producing site-less events (every pre-ADR-0038 caller).
func TestStandardEvents_WithSite(t *testing.T) {
	defined := shared.NewLaborStandardDefined(eventTime, "std-1", shared.Pick, 45, nil, eventTime)
	if defined.SiteCode != "" {
		t.Fatalf("constructor must be site-less, got %q", defined.SiteCode)
	}
	scoped := defined.WithSite("SITE-GRU-01")
	if scoped.SiteCode != "SITE-GRU-01" || defined.SiteCode != "" {
		t.Fatalf("WithSite: scoped=%q original=%q", scoped.SiteCode, defined.SiteCode)
	}
	scoped.SiteCode = defined.SiteCode
	if scoped.StandardId != defined.StandardId || scoped.ExpectedSeconds != defined.ExpectedSeconds || scoped.TaskType != defined.TaskType {
		t.Fatalf("WithSite changed another field: %+v vs %+v", scoped, defined)
	}

	revised := shared.NewLaborStandardRevised(eventTime, "std-2", shared.Pick, 45, 40, nil, eventTime)
	if revised.SiteCode != "" {
		t.Fatalf("constructor must be site-less, got %q", revised.SiteCode)
	}
	rs := revised.WithSite("SITE-GRU-01")
	if rs.SiteCode != "SITE-GRU-01" || revised.SiteCode != "" || rs.NewExpectedSeconds != 40 || rs.PreviousExpectedSeconds != 45 {
		t.Fatalf("WithSite on Revised: %+v (original %+v)", rs, revised)
	}
}
