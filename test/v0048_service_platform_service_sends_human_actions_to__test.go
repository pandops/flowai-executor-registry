package test

import (
	"strings"
	"testing"
	"time"
)

// Requirement: service-platform service sends human actions to audit and others to journal.
// "Forwarded administrator action is audited by its owner" is not
// applicable to executor-registry: it serves no admin operation.

func TestHumanActionsToAudit(t *testing.T) {
	t.Run("Operator cancels a task", func(t *testing.T) {
		// executor-registry form: each operator action publishes one audit message.
		h := newHarness(t)
		h.create(3, "gpu", typeDocker)
		h.reissue(3, 1)
		h.remove(3, 1)
		for _, a := range []string{"executor_created", "executor_token_reissued", "executor_deleted"} {
			if n := len(h.audits(a)); n != 1 {
				t.Errorf("%s audits = %d", a, n)
			}
		}
		h.checkOutbox()
	})

	t.Run("Rejected human action is audited", func(t *testing.T) {
		h := newHarness(t)
		h.create(3, "gpu", typeDocker)
		h.broker.FailAccountUpdate = errTest
		h.remove(3, 1)
		a := h.audits("executor_deleted")
		if len(a) != 1 || a[0].Body["outcome"] != "rejected" || a[0].Body["reason"] != "service_unavailable" {
			t.Fatalf("audit %v", a)
		}
		if len(h.store.Executors()) != 1 {
			t.Fatal("rejected delete changed data")
		}
	})

	t.Run("Executor event goes to journal", func(t *testing.T) {
		h := newHarness(t)
		h.create(3, "gpu", typeDocker)
		n := len(h.outbox())
		h.heartbeat(1, defaultHB())
		h.advance(11 * time.Minute)
		h.sweep()
		h.heartbeat(1, ipHB("10.1.1.1"))
		got := map[string]int{}
		for _, m := range h.outbox()[n:] {
			if !strings.HasPrefix(m.Subject, "executor-registry.event.") {
				t.Errorf("heartbeat-derived message on %s", m.Subject)
			}
			got[m.Subject]++
		}
		// The heartbeats, the loss and the restore produced these events, and no audit message.
		for _, kind := range []string{"executor_status", "connection_lost", "executor_lost", "connection_restored", "ip_changed"} {
			if got["executor-registry.event."+kind+".pub"] == 0 {
				t.Errorf("no %s event in %v", kind, got)
			}
		}
		if len(h.audits("executor_created")) != 1 || len(h.audits("executor_token_reissued")) != 0 || len(h.audits("executor_deleted")) != 0 {
			t.Error("heartbeat-derived change was audited")
		}
	})
}

type testErr string

func (e testErr) Error() string { return string(e) }

const errTest = testErr("account update refused")
