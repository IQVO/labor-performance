package mcp

import (
	"strings"
	"testing"
)

// get_task_type_utilization's description is the contract an agent reads.
// The open gap is computed per associate only (ADR 0014), so the task-type
// tool must say openGapSeconds is always 0 rather than promising the
// still-running gap (audit finding: the description promised it, the use
// case never delivered it).
func TestGovernance_TaskTypeUtilizationDescriptionDoesNotPromiseOpenGap(t *testing.T) {
	for _, tool := range governanceTools(t) {
		if tool.Name != "get_task_type_utilization" {
			continue
		}
		if !strings.Contains(tool.Description, "openGapSeconds is always 0") {
			t.Fatalf("description must state openGapSeconds is always 0 at task-type scope, got: %s", tool.Description)
		}
		if strings.Contains(tool.Description, "the still-running open gap for anyone currently idle") {
			t.Fatalf("description still promises the open gap for anyone currently idle: %s", tool.Description)
		}
		return
	}
	t.Fatal("get_task_type_utilization is not registered")
}
