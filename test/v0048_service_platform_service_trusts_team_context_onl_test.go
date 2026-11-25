package test

import (
	"fmt"
	"testing"

	"github.com/pandops/flowai-executor-registry/internal/broker"
)

// Requirement: service-platform service trusts team context only from ui requests.

func TestTeamContext(t *testing.T) {
	t.Run("Gateway forwards verified operator context", func(t *testing.T) {
		h := newHarness(t)
		hd := uiHeader(3, 501)
		hd["team_name"] = []string{"team-4"} // display-only, never authorizes
		r := h.uiRaw("create", hd, envelope(map[string]any{"team_id": 3}, map[string]any{"execution_tag": "gpu", "executor_type": typeDocker}))
		if r.Status != 201 || num(r.Body["team_id"]) != 3 {
			t.Fatalf("create %d %s", r.Status, r.Raw)
		}
		hd = uiHeader(3, 502)
		delete(hd, "team_name")
		if r := h.uiRaw("get", hd, envelope(map[string]any{"team_id": 3, "executor_id": 1}, nil)); r.Status != 200 {
			t.Fatalf("get without team_name %d", r.Status)
		}
		a := h.audits("executor_created")
		if num(a[0].Body["request_id"]) != 501 || num(a[0].Body["actor_id"]) != testOperatorID || a[0].Header["request_id"] != "501" {
			t.Fatalf("audit context %s %v", a[0].Raw, a[0].Header)
		}
	})

	t.Run("Payload names another team", func(t *testing.T) {
		h := newHarness(t)
		h.create(4, "gpu", typeDocker)
		for _, c := range []struct {
			op     string
			params map[string]any
			body   any
		}{
			{"create", map[string]any{"team_id": 4}, map[string]any{"execution_tag": "gpu", "executor_type": typeDocker}},
			{"create", map[string]any{"team_id": 3}, map[string]any{"execution_tag": "gpu", "executor_type": typeDocker, "team_id": 4}},
			{"get", map[string]any{"team_id": 4, "executor_id": 1}, nil},
			{"list", map[string]any{"team_id": 4}, nil},
			{"reissue", map[string]any{"team_id": 4, "executor_id": 1}, nil},
			{"delete", map[string]any{"team_id": 4, "executor_id": 1}, nil},
		} {
			r := h.ui(c.op, 3, c.params, c.body)
			if r.Status != 400 && r.Status != 404 {
				t.Errorf("%s %v: %d %s", c.op, c.params, r.Status, r.Raw)
			}
			if _, has := r.Body["items"]; has {
				t.Errorf("%s returned team 4 data", c.op)
			}
		}
		if len(h.store.Executors()) != 1 || len(h.broker.AccountPushes) != 0 || len(h.broker.RequestsTo(subjTeamGet)) != 1 {
			t.Fatal("team 4 data read or changed")
		}
	})

	t.Run("Context is missing", func(t *testing.T) {
		h := newHarness(t)
		h.create(3, "gpu", typeDocker)
		for _, drop := range []string{"team", "operator_id", "request_id"} {
			for _, op := range []string{"list", "get", "create", "list-execution-tags"} {
				hd := uiHeader(3, 9)
				delete(hd, drop)
				r := h.uiRaw(op, hd, envelope(map[string]any{"team_id": 3, "executor_id": 1}, map[string]any{"execution_tag": "gpu", "executor_type": typeDocker}))
				if r.Status != 400 || r.code() != "invalid_request" {
					t.Errorf("%s without %s: %d %s", op, drop, r.Status, r.Raw)
				}
				// D12 owner decision 2026-11-09: no valid request_id, no
				// request_id field; every other error keeps it.
				rid, has := r.Body["request_id"]
				if drop == "request_id" && has {
					t.Errorf("%s without request_id: %s, want no request_id field", op, r.Raw)
				}
				if drop != "request_id" && fmt.Sprint(rid) != "9" {
					t.Errorf("%s without %s: %s, want request_id 9", op, drop, r.Raw)
				}
			}
		}
		for _, bad := range []string{"0", "-1", "09", "r-1"} {
			hd := uiHeader(3, 9)
			hd["request_id"] = []string{bad}
			if r := h.uiRaw("list", hd, envelope(map[string]any{"team_id": 3}, nil)); r.Status != 400 || hasField(r, "request_id") {
				t.Errorf("request_id header %q: %d %s, want 400 without request_id", bad, r.Status, r.Raw)
			}
		}
		for _, team := range []string{"admin", "abc", "0", "-3", "03"} {
			hd := uiHeader(3, 9)
			hd["team"] = []string{team}
			if r := h.uiRaw("list", hd, envelope(map[string]any{"team_id": 3}, nil)); r.Status != 400 || r.code() != "invalid_request" {
				t.Errorf("team header %q: %d", team, r.Status)
			}
		}
		if len(h.store.Executors()) != 1 || len(h.audits("executor_created")) != 1 {
			t.Fatal("request without context changed data or was audited")
		}
		// service requests: team header and params must agree.
		if r := h.handle("executor-registry.service.team-tags.sub", broker.Header{"request_id": {"1"}}, envelope(map[string]any{"team_id": 3}, nil)); r.Status != 400 || fmt.Sprint(r.Body["request_id"]) != "1" {
			t.Errorf("team-tags without team header: %d %s, want 400 with request_id 1", r.Status, r.Raw)
		}
		if r := h.handle("executor-registry.service.team-tags.sub", broker.Header{}, envelope(map[string]any{"team_id": 3}, nil)); r.Status != 400 || hasField(r, "request_id") {
			t.Errorf("team-tags without team and request_id headers: %d %s, want 400 without request_id", r.Status, r.Raw)
		}
		// An Executor heartbeat without request_id: its error omits the field.
		if r := h.handle("executor-registry.executor.999.heartbeat.sub", broker.Header{"team": {"3"}}, envelope(nil, defaultHB().body())); r.Status < 400 || hasField(r, "request_id") {
			t.Errorf("heartbeat of an unknown Executor without request_id: %d %s, want an error without request_id", r.Status, r.Raw)
		}
	})
}

func hasField(r reply, k string) bool {
	_, ok := r.Body[k]
	return ok
}
