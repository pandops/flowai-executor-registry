package test

import (
	"encoding/json"
	"strings"
	"testing"
	"time"
)

// Requirement: executor-registry serves team-filtered Executor reads.

func TestTeamFilteredReads(t *testing.T) {
	t.Run("Read returns the reported IP without a name", func(t *testing.T) {
		h := newHarness(t)
		for i := 0; i < 12; i++ {
			h.create(3, "gpu", typeDocker)
		}
		h.now = time.Date(2026, 10, 15, 9, 44, 0, 0, time.UTC)
		h.heartbeat(7, hb{MaxParallel: 2, Interval: 15, Miss: 3, Wait: 600, IP: "192.0.2.10"})
		r7, r12 := h.get(3, 7), h.get(3, 12)
		h.validate("ui.get.reply.schema.json", r7.Raw)
		h.validate("ui.get.reply.schema.json", r12.Raw)
		if r7.Body["reported_ip"] != "192.0.2.10" || r7.Body["ip_reported_at"] != "2026-10-15T09:44:00.000Z" {
			t.Fatalf("executor 7 = %s", r7.Raw)
		}
		if v, ok := r12.Body["reported_ip"]; !ok || v != nil {
			t.Fatalf("executor 12 reported_ip = %s", r12.Raw)
		}
		if v, ok := r12.Body["ip_reported_at"]; !ok || v != nil {
			t.Fatalf("executor 12 ip_reported_at = %s", r12.Raw)
		}
		for _, r := range []reply{r7, r12, h.ui("list", 3, map[string]any{"team_id": 3}, nil)} {
			for _, f := range []string{`"name"`, `"display_name"`, `"scope"`, `"token"`, `"connection_state"`, `"available_capacity"`} {
				if strings.Contains(string(r.Raw), f) {
					t.Fatalf("read carries %s: %s", f, r.Raw)
				}
			}
		}
		for _, f := range []string{"executor_id", "team_id", "execution_tag", "executor_type", "created_at", "token_fingerprint", "token_issued_at"} {
			if _, ok := r7.Body[f]; !ok {
				t.Fatalf("read lacks %s", f)
			}
		}
	})

	t.Run("Team list shows only own Executors", func(t *testing.T) {
		h := newHarness(t)
		h.create(3, "gpu", typeDocker)
		h.create(4, "gpu", typeDocker)
		h.create(3, "cpu", typeDocker)
		h.create(4, "cpu", typeDocker)
		h.create(3, "gpu", typeDocker)
		r := h.ui("list", 3, map[string]any{"team_id": 3, "limit": 2}, nil)
		h.validate("ui.list.reply.schema.json", r.Raw)
		ids := listIDs(r)
		if len(ids) != 2 || ids[0] != 1 || ids[1] != 3 || num(r.Body["total_count"]) != 3 {
			t.Fatalf("page 1 = %s", r.Raw)
		}
		cur, _ := r.Body["next_cursor"].(string)
		if cur == "" {
			t.Fatal("no next_cursor")
		}
		r2 := h.ui("list", 3, map[string]any{"team_id": 3, "limit": 2, "cursor": cur}, nil)
		ids = listIDs(r2)
		if len(ids) != 1 || ids[0] != 5 || r2.Body["next_cursor"] != nil || num(r2.Body["total_count"]) != 3 {
			t.Fatalf("page 2 = %s", r2.Raw)
		}
		for _, e := range r.Body["items"].([]any) {
			if num(e.(map[string]any)["team_id"]) != 3 {
				t.Fatal("foreign Executor listed")
			}
		}
	})

	t.Run("Foreign Executor detail is not revealed", func(t *testing.T) {
		h := newHarness(t)
		for i := 0; i < 8; i++ {
			h.create(3, "gpu", typeDocker)
		}
		h.create(4, "gpu", typeDocker)
		foreign, unknown := h.get(3, 9), h.get(3, 99)
		if foreign.Status != 404 || foreign.code() != "executor_unknown_or_unavailable" || foreign.Body["message"] != unknown.Body["message"] || unknown.Status != 404 {
			t.Fatalf("foreign %s unknown %s", foreign.Raw, unknown.Raw)
		}
	})

	t.Run("Non-integer Executor identifier is rejected", func(t *testing.T) {
		for _, raw := range []string{`"3f2c9a1e-0b7d-4c55-9a51-2f0e6d1c8b44"`, `"e-1"`, `0`, `-7`, `7.5`, `"007"`, `"7"`, `9223372036854775808`, `1e1`} {
			for _, op := range []string{"get", "reissue", "delete"} {
				h := newHarness(t)
				for i := 0; i < 7; i++ {
					h.create(3, "gpu", typeDocker)
				}
				outbox := len(h.outbox())
				data := []byte(`{"params":{"team_id":3,"executor_id":` + raw + `}}`)
				r := h.uiRaw(op, uiHeader(3, 5), data)
				if r.Status != 400 || r.code() != "invalid_request" {
					t.Errorf("%s %s: %d %s", op, raw, r.Status, r.Raw)
				}
				if len(h.broker.AccountPushes) != 0 || len(h.store.Executors()) != 7 {
					t.Errorf("%s %s: changed state", op, raw)
				}
				for _, m := range h.outbox()[outbox:] {
					if !strings.HasPrefix(m.Subject, "executor-registry.audit.") {
						t.Errorf("%s %s: published %s", op, raw, m.Subject)
					}
				}
			}
		}
	})

	t.Run("Service looks up an Executor type", func(t *testing.T) {
		h := newHarness(t)
		for i := 0; i < 6; i++ {
			h.create(3, "cpu", typeK8s)
		}
		h.create(3, "gpu", typeDocker)
		hd := map[string][]string{"team": {"3"}, "request_id": {"77"}}
		r := h.handle("executor-registry.service.executor-type.sub", hd, envelope(map[string]any{"executor_id": 7}, nil))
		if r.Status != 200 {
			t.Fatalf("executor-type = %d %s", r.Status, r.Raw)
		}
		h.validate("service.executor-type.reply.schema.json", r.Raw)
		if num(r.Body["team_id"]) != 3 || num(r.Body["executor_id"]) != 7 || r.Body["execution_tag"] != "gpu" || r.Body["executor_type"] != typeDocker {
			t.Fatalf("executor-type = %s", r.Raw)
		}
		// An Executor of another team is not found under team 4.
		r = h.handle("executor-registry.service.executor-type.sub", map[string][]string{"team": {"4"}, "request_id": {"78"}}, envelope(map[string]any{"executor_id": 7}, nil))
		if r.Status != 404 || r.code() != "executor_unknown_or_unavailable" {
			t.Fatalf("foreign executor-type = %d %s", r.Status, r.Raw)
		}
	})
}

func listIDs(r reply) []int64 {
	var out []int64
	items, _ := r.Body["items"].([]any)
	for _, it := range items {
		out = append(out, num(it.(map[string]any)["executor_id"]))
	}
	return out
}

var _ = json.Marshal
