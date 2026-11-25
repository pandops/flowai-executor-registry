package test

import (
	"strings"
	"testing"
)

// Requirement: executor-registry audits every human Executor action.

func TestAuditHumanActions(t *testing.T) {
	t.Run("Executor creation is audited", func(t *testing.T) {
		h := newHarness(t)
		_, tok := h.create(3, "gpu", typeDocker)
		a := h.audits("executor_created")
		if len(a) != 1 {
			t.Fatalf("audits = %d", len(a))
		}
		b := a[0].Body
		if num(b["team_id"]) != 3 || num(b["actor_id"]) != testOperatorID || b["actor_type"] != "operator" || b["action"] != "executor_created" ||
			b["resource_type"] != "executor" || num(b["resource_id"]) != 1 || b["outcome"] != "succeeded" || b["source_service"] != "executor-registry" || num(b["request_id"]) <= 0 {
			t.Fatalf("audit = %s", a[0].Raw)
		}
		if a[0].Header["team"] != "3" || a[0].Header["request_id"] == "" {
			t.Fatalf("audit headers = %v", a[0].Header)
		}
		if strings.Contains(string(a[0].Raw), tok[strings.LastIndex(tok, ".")+1:]) {
			t.Fatal("audit carries the JWT")
		}
		h.checkOutbox()
		// A rejected create is audited too, without an Executor id.
		h.ui("create", 3, map[string]any{"team_id": 3}, map[string]any{"execution_tag": "g.pu", "executor_type": typeDocker})
		a = h.audits("executor_created")
		if len(a) != 2 || a[1].Body["outcome"] != "rejected" || a[1].Body["reason"] != "invalid_request" || a[1].Body["resource_id"] != nil {
			t.Fatalf("rejected create audit = %v", a)
		}
	})

	t.Run("Rejected reissue is audited", func(t *testing.T) {
		h := newHarness(t)
		for i := 0; i < 8; i++ {
			h.create(4, "gpu", typeDocker)
		}
		h.create(4, "arm", typeDocker) // executor 9 of team 4
		r := h.reissue(3, 9)
		if r.Status != 404 {
			t.Fatalf("reissue = %d", r.Status)
		}
		a := h.audits("executor_token_reissued")
		if len(a) != 1 || a[0].Body["outcome"] != "rejected" || num(a[0].Body["team_id"]) != 3 || a[0].Header["team"] != "3" {
			t.Fatalf("audit = %v", a)
		}
		if a[0].Body["resource_id"] != nil || strings.Contains(string(a[0].Raw), "arm") || strings.Contains(string(a[0].Raw), `"team_id":4`) {
			t.Fatalf("audit reveals the foreign Executor: %s", a[0].Raw)
		}
		h.checkOutbox()
	})

	t.Run("Create and delete publish machine events", func(t *testing.T) {
		h := newHarness(t)
		for i := 0; i < 6; i++ {
			h.create(4, "cpu", typeDocker)
		}
		h.create(3, "gpu", typeDocker)
		h.remove(3, 7)
		reg, rem := h.eventsFor("executor_registered", 7), h.eventsFor("executor_removed", 7)
		if len(reg) != 1 || len(rem) != 1 || reg[0].Seq >= rem[0].Seq {
			t.Fatalf("events: registered %v removed %v", reg, rem)
		}
		for _, e := range []outMsg{reg[0], rem[0]} {
			if num(e.Body["team_id"]) != 3 || e.Body["execution_tag"] != "gpu" || e.Body["executor_type"] != typeDocker || e.Body["occurred_at"] == nil {
				t.Fatalf("event = %s", e.Raw)
			}
			if _, has := e.Body["name"]; has {
				t.Fatal("event carries a name")
			}
		}
		// Each event is in the same transaction as its audit message: adjacent outbox rows.
		ob := h.outbox()
		for i, m := range ob {
			if m.Seq == reg[0].Seq || m.Seq == rem[0].Seq {
				var pair outMsg
				if i > 0 && strings.Contains(ob[i-1].Subject, ".audit.") {
					pair = ob[i-1]
				} else if i+1 < len(ob) {
					pair = ob[i+1]
				}
				want := "executor-registry.audit.executor_created.pub"
				if m.Seq == rem[0].Seq {
					want = "executor-registry.audit.executor_deleted.pub"
				}
				if pair.Subject != want || num(pair.Body["resource_id"]) != 7 || pair.Body["outcome"] != "succeeded" {
					t.Fatalf("event %s not committed with %s: %s", m.Subject, want, pair.Subject)
				}
			}
		}
		h.checkOutbox()
	})

	t.Run("Reissue publishes no machine event", func(t *testing.T) {
		h := newHarness(t)
		h.create(3, "gpu", typeDocker)
		before := len(h.outbox())
		h.reissue(3, 1)
		after := h.outbox()[before:]
		if len(after) != 1 || after[0].Subject != "executor-registry.audit.executor_token_reissued.pub" || after[0].Body["outcome"] != "succeeded" {
			t.Fatalf("reissue published %v", after)
		}
		h.checkOutbox()
	})
}
