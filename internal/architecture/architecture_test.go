// Package architecture holds fitness tests (the Go analogue of ArchUnit,
// via github.com/arch-go/arch-go) that enforce the hexagonal/ports-and-adapters
// dependency rule described in the project's CLAUDE.md: dependencies point
// inward only, and inbound/outbound adapters never depend on each other.
//
// This service now HAS an analytics data-mesh side —
// internal/analytics/report, added by ADR-0007 for fleet parity with the
// sibling services' data products — so the two analytics-specific rules
// the siblings enforce (analytics-depends-on-nothing-but-itself,
// OLTP-must-not-import-analytics) are asserted here too. They were
// deliberately absent from this file's first version, when no such region
// existed to constrain.
package architecture

import (
	"testing"

	archgo "github.com/arch-go/arch-go/api"
	"github.com/arch-go/arch-go/api/configuration"
)

const modulePath = "github.com/claudioed/labor-performance"

func TestHexagonalArchitecture(t *testing.T) {
	moduleInfo := configuration.Load(modulePath)

	// arch-go's package-pattern DSL uses '.' as the path-segment separator
	// (mirroring Java package notation), not '/': "**.internal.domain.**"
	// matches any Go import path containing an internal/domain segment,
	// e.g. github.com/claudioed/labor-performance/internal/domain/standard.

	t.Run("domain depends on nothing internal except domain", func(t *testing.T) {
		rule := &configuration.DependenciesRule{
			Package: "**.internal.domain.**",
			ShouldOnlyDependsOn: &configuration.Dependencies{
				Internal: []string{"**.internal.domain.**"},
			},
		}

		result := archgo.CheckArchitecture(moduleInfo, configuration.Config{
			DependenciesRules: []*configuration.DependenciesRule{rule},
		})

		assertPass(t, result)
	})

	t.Run("application depends only on domain", func(t *testing.T) {
		rule := &configuration.DependenciesRule{
			Package: "**.internal.application.**",
			ShouldOnlyDependsOn: &configuration.Dependencies{
				Internal: []string{
					"**.internal.domain.**",
					"**.internal.application.**",
				},
			},
		}

		result := archgo.CheckArchitecture(moduleInfo, configuration.Config{
			DependenciesRules: []*configuration.DependenciesRule{rule},
		})

		assertPass(t, result)
	})

	t.Run("inbound adapters do not depend on outbound adapters", func(t *testing.T) {
		rule := &configuration.DependenciesRule{
			Package: "**.internal.adapters.inbound.**",
			ShouldNotDependsOn: &configuration.Dependencies{
				Internal: []string{"**.internal.adapters.outbound.**"},
			},
		}

		result := archgo.CheckArchitecture(moduleInfo, configuration.Config{
			DependenciesRules: []*configuration.DependenciesRule{rule},
		})

		assertPass(t, result)
	})

	t.Run("outbound adapters do not depend on inbound adapters", func(t *testing.T) {
		rule := &configuration.DependenciesRule{
			Package: "**.internal.adapters.outbound.**",
			ShouldNotDependsOn: &configuration.Dependencies{
				Internal: []string{"**.internal.adapters.inbound.**"},
			},
		}

		result := archgo.CheckArchitecture(moduleInfo, configuration.Config{
			DependenciesRules: []*configuration.DependenciesRule{rule},
		})

		assertPass(t, result)
	})

	t.Run("the analytics read model depends on nothing but itself", func(t *testing.T) {
		// internal/analytics/report is the analytical read-model region
		// added in ADR-0007. Its whole value is being derivable from the
		// event stream alone: if it could reach into the OLTP domain or
		// application layers, the report would silently become coupled to
		// the transactional model it is supposed to be independent of, and
		// "rebuild the read model by replaying the topic" would stop being
		// true.
		rule := &configuration.DependenciesRule{
			Package: "**.internal.analytics.**",
			ShouldOnlyDependsOn: &configuration.Dependencies{
				Internal: []string{"**.internal.analytics.**"},
			},
		}

		result := archgo.CheckArchitecture(moduleInfo, configuration.Config{
			DependenciesRules: []*configuration.DependenciesRule{rule},
		})

		assertPass(t, result)
	})

	t.Run("the OLTP domain and application layers do not import analytics", func(t *testing.T) {
		// The other half of the same isolation: the analytics data
		// product must remain strictly additive from the OLTP write
		// path's point of view. The domain rule above already forbids
		// this transitively, but stating it directly means a future
		// loosening of that rule cannot quietly let analytics leak
		// inward.
		rule := &configuration.DependenciesRule{
			Package: "**.internal.domain.**",
			ShouldNotDependsOn: &configuration.Dependencies{
				Internal: []string{"**.internal.analytics.**"},
			},
		}
		appRule := &configuration.DependenciesRule{
			Package: "**.internal.application.**",
			ShouldNotDependsOn: &configuration.Dependencies{
				Internal: []string{"**.internal.analytics.**"},
			},
		}

		result := archgo.CheckArchitecture(moduleInfo, configuration.Config{
			DependenciesRules: []*configuration.DependenciesRule{rule, appRule},
		})

		assertPass(t, result)
	})

	t.Run("only cmd is the composition root wiring every layer", func(t *testing.T) {
		// Nothing under internal/** may import cmd/**: if it did, cmd would
		// no longer be a leaf composition root but a dependency of the very
		// layers it is supposed to wire together.
		rule := &configuration.DependenciesRule{
			Package: "**.internal.**",
			ShouldNotDependsOn: &configuration.Dependencies{
				Internal: []string{"**.cmd.**"},
			},
		}

		result := archgo.CheckArchitecture(moduleInfo, configuration.Config{
			DependenciesRules: []*configuration.DependenciesRule{rule},
		})

		assertPass(t, result)
	})

	// DEVIATION FROM inventory-storage (documented, not silently dropped):
	// inventory-storage's arch-fitness suite also asserts "ports package
	// only contains interfaces". This service's ports.go, unlike
	// inventory-storage's, ALSO declares the Scorecard/TaskTypeBreakdown/
	// TaskTypePerformance read-model structs — the return types of the
	// StandardRepo/PerformanceRepo interfaces declared in that same file,
	// written in this service's original v1 build (pre-dates this
	// fleet-parity task). arch-go's ShouldOnlyContainInterfaces rule fails
	// on that file as a result. Splitting those structs into a separate
	// package would require touching 5 already-built, already-tested
	// files (server.go, both performance_repo.go adapters, and two
	// usecases), which conflicts with this task's explicit "additive-only:
	// nothing about the existing domain/application code changes"
	// instruction — so this rule is intentionally NOT included here,
	// mirroring the same "adapt the rule set to what's actually true for
	// this codebase, and say so" judgment call FLEET_PARITY_TASK.md
	// documents for inventory-storage's two analytics-specific rules.
}

func assertPass(t *testing.T, result *archgo.Result) {
	t.Helper()

	if result.Pass {
		return
	}

	if result.DependenciesRuleResult != nil {
		for _, r := range result.DependenciesRuleResult.Results {
			if !r.Passes {
				t.Errorf("%s", archViolation("dependency", r.Description, r.Verifications))
			}
		}
	}

	if result.ContentsRuleResult != nil {
		for _, r := range result.ContentsRuleResult.Results {
			if !r.Passes {
				t.Errorf("%s", archViolation("contents", r.Description, r.Verifications))
			}
		}
	}

	t.FailNow()
}
