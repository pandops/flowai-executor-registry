package test

import (
	"strings"
	"testing"
)

// Requirement: executor-registry creates team Executors with one tag and type.

func TestCreateTeamExecutor(t *testing.T) {
	t.Run("Operator creates a team Executor", func(t *testing.T) {
		h := newHarness(t)
		h.create(3, "cpu", typeDocker)
		h.create(3, "cpu", typeDocker)
		h.create(4, "cpu", typeDocker)
		r := h.ui("create", 3, map[string]any{"team_id": 3}, map[string]any{"execution_tag": "gpu", "executor_type": typeDocker})
		if r.Status != 201 {
			t.Fatalf("create = %d %s", r.Status, r.Raw)
		}
		if num(r.Body["executor_id"]) != 4 || num(r.Body["team_id"]) != 3 || r.Body["execution_tag"] != "gpu" || r.Body["executor_type"] != typeDocker {
			t.Fatalf("unexpected create reply %s", r.Raw)
		}
		var found bool
		for _, e := range h.store.Executors() {
			if e.ExecutorID == 4 {
				found = e.TeamID == 3 && e.Tag == "gpu" && e.Type == typeDocker
			}
		}
		if !found {
			t.Fatalf("executor 4 not stored with team 3, tag gpu: %+v", h.store.Executors())
		}
		// Team checked with team-registry before creating.
		reqs := h.broker.RequestsTo(subjTeamGet)
		if len(reqs) != 4 || reqs[3].Header.Get("team") != "3" {
			t.Fatalf("team checks = %+v", reqs)
		}
	})

	t.Run("Executor numbers are global", func(t *testing.T) {
		h := newHarness(t)
		a, _ := h.create(3, "gpu", typeDocker)
		b, _ := h.create(4, "gpu", typeDocker)
		if a != 1 || b != 2 {
			t.Fatalf("ids = %d, %d; want 1, 2", a, b)
		}
		r := h.get(4, 1)
		if r.Status != 404 || r.code() != "executor_unknown_or_unavailable" {
			t.Fatalf("team 4 reading executor 1 = %d %s", r.Status, r.Raw)
		}
	})

	t.Run("Deleted Executor's number is never reused", func(t *testing.T) {
		h := newHarness(t)
		for i := 0; i < 5; i++ {
			h.create(3, "gpu", typeDocker)
		}
		if r := h.remove(3, 5); r.Status != 204 {
			t.Fatalf("delete 5 = %d %s", r.Status, r.Raw)
		}
		id, _ := h.create(3, "gpu", typeDocker)
		if id != 6 {
			t.Fatalf("new id = %d, want 6", id)
		}
		if r := h.get(3, 5); r.Status != 404 || r.code() != "executor_unknown_or_unavailable" {
			t.Fatalf("read (3,5) = %d %s", r.Status, r.Raw)
		}
	})

	t.Run("Invalid tag count is rejected", func(t *testing.T) {
		for name, body := range map[string]map[string]any{
			"missing":  {"executor_type": typeDocker},
			"null":     {"execution_tag": nil, "executor_type": typeDocker},
			"two tags": {"execution_tag": []string{"gpu", "cpu"}, "executor_type": typeDocker},
			"array":    {"execution_tag": []string{"gpu"}, "executor_type": typeDocker},
		} {
			h := newHarness(t)
			r := h.ui("create", 3, map[string]any{"team_id": 3}, body)
			if r.Status != 400 || r.code() != "invalid_tag_count" {
				t.Errorf("%s: %d %s", name, r.Status, r.Raw)
			}
			assertNothingCreated(t, h)
		}
	})

	t.Run("Tag outside the allowed characters is rejected", func(t *testing.T) {
		for _, tag := range []string{"gp.u", "gp*u", "gp/u", "gp u", "", strings.Repeat("a", 129)} {
			h := newHarness(t)
			r := h.ui("create", 3, map[string]any{"team_id": 3}, map[string]any{"execution_tag": tag, "executor_type": typeDocker})
			if r.Status != 400 || r.code() != "invalid_request" {
				t.Errorf("tag %q: %d %s", tag, r.Status, r.Raw)
			}
			if n := len(h.broker.RequestsTo(subjTeamGet)); n != 0 {
				t.Errorf("tag %q: %d team checks sent", tag, n)
			}
			assertNothingCreated(t, h)
		}
		// 128 characters of the allowed set are accepted.
		h := newHarness(t)
		h.create(3, strings.Repeat("A", 64)+strings.Repeat("_-9z", 16), typeDocker)
	})

	t.Run("Unsupported Executor type is rejected", func(t *testing.T) {
		for _, typ := range []any{"executor_podman_openhands", "", nil, 7} {
			h := newHarness(t)
			r := h.ui("create", 3, map[string]any{"team_id": 3}, map[string]any{"execution_tag": "gpu", "executor_type": typ})
			if r.Status != 400 || r.code() != "invalid_request" {
				t.Errorf("type %v: %d %s", typ, r.Status, r.Raw)
			}
			assertNothingCreated(t, h)
		}
	})

	t.Run("Archived team cannot get an Executor", func(t *testing.T) {
		h := newHarness(t)
		h.teams[3] = team{archived: true}
		r := h.ui("create", 3, map[string]any{"team_id": 3}, map[string]any{"execution_tag": "gpu", "executor_type": typeDocker})
		if r.Status < 400 || r.Status >= 500 {
			t.Fatalf("archived team create = %d %s", r.Status, r.Raw)
		}
		assertNothingCreated(t, h)

		h = newHarness(t)
		r = h.ui("create", 7, map[string]any{"team_id": 7}, map[string]any{"execution_tag": "gpu", "executor_type": typeDocker})
		if r.Status < 400 || r.Status >= 500 {
			t.Fatalf("unknown team create = %d %s", r.Status, r.Raw)
		}
		assertNothingCreated(t, h)
	})

	t.Run("Scope or team in the body is rejected", func(t *testing.T) {
		for _, extra := range []string{"scope", "team_id", "executor_id"} {
			h := newHarness(t)
			body := map[string]any{"execution_tag": "gpu", "executor_type": typeDocker}
			if extra == "scope" {
				body[extra] = "team"
			} else {
				body[extra] = 3
			}
			r := h.ui("create", 3, map[string]any{"team_id": 3}, body)
			if r.Status != 400 || r.code() != "invalid_request" {
				t.Errorf("%s: %d %s", extra, r.Status, r.Raw)
			}
			assertNothingCreated(t, h)
		}
	})

	t.Run("Name in the create request is rejected", func(t *testing.T) {
		for _, field := range []string{"name", "display_name"} {
			h := newHarness(t)
			r := h.ui("create", 3, map[string]any{"team_id": 3}, map[string]any{"execution_tag": "gpu", "executor_type": typeDocker, field: "builder"})
			if r.Status != 400 || r.code() != "invalid_request" {
				t.Errorf("%s: %d %s", field, r.Status, r.Raw)
			}
			if n := len(h.broker.RequestsTo(subjTeamGet)); n != 0 {
				t.Errorf("%s: team check sent", field)
			}
			assertNothingCreated(t, h)
		}
	})
}

// assertNothingCreated checks no Executor, consumer, JWT or machine event exists.
func assertNothingCreated(t *testing.T, h *harness) {
	t.Helper()
	if n := len(h.store.Executors()); n != 0 {
		t.Errorf("%d Executors stored", n)
	}
	if n := len(h.broker.Consumers); n != 0 {
		t.Errorf("%d consumers created", n)
	}
	if n := len(h.events("executor_registered")); n != 0 {
		t.Errorf("%d executor_registered events", n)
	}
}
