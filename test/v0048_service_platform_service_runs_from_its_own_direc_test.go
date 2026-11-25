package test

import (
	"testing"
)

// Requirement: service-platform service runs from its own directory and image.
// "E2E uses the deployed services" is not applicable to executor-registry
// (an E2E-suite rule, qa-e2e/full-flow).

func TestOwnDirectory(t *testing.T) {
	t.Run("Change to one service selects only its tests", func(t *testing.T) {
		for _, f := range []string{"svc/executor-registry/internal/app/app.go", "svc/executor-registry/test/harness_test.go", "svc/executor-registry/Containerfile"} {
			if got := changedOwners(t, f); got != `["executor-registry"]` {
				t.Errorf("%s selects %s", f, got)
			}
		}
	})
}
