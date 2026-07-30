//go:build unit

package admin_test

import (
	"os"
	"strings"
	"testing"
)

// TestReleasePolicy_BreakingChangesStayMinor guards the /v2 release policy.
//
// Go module majors live in the import path, so a major is cut by hand: rename
// the module path, then tag vN.0.0. CI must never auto-major. A `major` rule on
// a /v2 line re-applies the v2 cut's own breaking commits on every run and
// computes a phantom v3 that Go cannot consume for a /v2 module path — which is
// how lib-commons wedged its beta channel until the same guard landed there.
//
// Both directions are asserted: the rule must be `minor`, and it must not be
// re-flipped to `major`. See the header comment in .releaserc.yml.
func TestReleasePolicy_BreakingChangesStayMinor(t *testing.T) {
	t.Parallel()

	content, err := os.ReadFile("../.releaserc.yml")
	if err != nil {
		t.Fatalf("read .releaserc.yml: %v", err)
	}

	// Collapse all whitespace (spaces, tabs, newlines) so the guard cannot be
	// bypassed by reformatting the rule across lines or with tabs.
	normalized := strings.Join(strings.Fields(string(content)), "")

	if !strings.Contains(normalized, `{breaking:true,release:"minor"}`) {
		t.Error(`.releaserc.yml must map breaking changes to "minor": Go majors are cut by hand via the module path`)
	}

	if strings.Contains(normalized, `{breaking:true,release:"major"}`) {
		t.Error(`.releaserc.yml maps breaking changes to "major": this auto-majors the /v2 line into a tag Go cannot consume`)
	}
}
