package test

import (
	"errors"
	"testing"
)

// Requirement: service-platform service publishes changes through an outbox.
// "Synchronous publications without an outbox" is not applicable to
// executor-registry (source-controller webhook intake and admin-panel).

func TestOutbox(t *testing.T) {
	t.Run("Rolled-back change publishes nothing", func(t *testing.T) {
		h := newHarness(t)
		h.store.FailNextCommit = errors.New("commit failed")
		r := h.ui("create", 3, map[string]any{"team_id": 3}, map[string]any{"execution_tag": "gpu", "executor_type": typeDocker})
		if r.Status != 503 || r.Body["token"] != nil {
			t.Fatalf("create with failing commit = %d %s", r.Status, r.Raw)
		}
		if len(h.events("executor_registered")) != 0 || len(h.store.Executors()) != 0 {
			t.Fatal("rolled-back create left an event or Executor")
		}
		for _, a := range h.audits("executor_created") {
			if a.Body["outcome"] == "succeeded" {
				t.Fatal("rolled-back create audited as succeeded")
			}
		}
		h.relay()
		for _, m := range h.broker.Published {
			if m.Subject == "executor-registry.event.executor_registered.pub" {
				t.Fatal("rolled-back event published")
			}
		}
	})
}
