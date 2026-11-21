package test

import (
	"errors"
	"sync"
	"testing"

	"github.com/pandops/flowai-executor-registry/internal/ids"
)

// Requirement: service-platform services use integer identifiers.
// executor_id is one global sequence (executor-registry spec, which wins
// over the per-team rule, agents.md rule 13). "Redelivered message is
// deduplicated by publisher sequence" is not applicable: executor-registry
// consumes no stream.

func TestIntegerIdentifiers(t *testing.T) {
	t.Run("Invalid identifier is rejected", func(t *testing.T) {
		for _, raw := range []string{`"abc"`, `0`, `-1`, `1.5`, `9223372036854775808`, `"3f2c8a10-6b1e-4d2a-9c3e-5a7b8c9d0e1f"`, `"3"`, `null`, `true`} {
			h := newHarness(t)
			h.create(3, "gpu", typeDocker)
			n := len(h.outbox())
			for _, op := range []string{"list", "create"} {
				data := []byte(`{"params":{"team_id":` + raw + `},"body":{"execution_tag":"gpu","executor_type":"executor_docker_openhands"}}`)
				r := h.uiRaw(op, uiHeader(3, 9), data)
				if r.Status != 400 || r.code() != "invalid_request" {
					t.Errorf("%s team_id=%s: %d %s", op, raw, r.Status, r.Raw)
				}
			}
			if len(h.store.Executors()) != 1 || len(h.broker.RequestsTo(subjTeamGet)) != 1 {
				t.Errorf("team_id=%s: wrote or called", raw)
			}
			for _, m := range h.outbox()[n:] {
				if m.Subject == "executor-registry.event.executor_registered.pub" {
					t.Errorf("team_id=%s: created", raw)
				}
			}
		}
		for s, want := range map[string]int64{"1": 1, "9223372036854775807": 9223372036854775807, "42": 42} {
			if got, ok := ids.Parse(s); !ok || got != want {
				t.Errorf("Parse(%q) = %d %v", s, got, ok)
			}
		}
		for _, s := range []string{"", "0", "-1", "+1", "01", "1.0", "1e3", "9223372036854775808", " 1", "abc"} {
			if _, ok := ids.Parse(s); ok {
				t.Errorf("Parse(%q) accepted", s)
			}
		}
	})

	t.Run("First record in each team is 1", func(t *testing.T) {
		// Executors are numbered globally: the first Executor of a new deployment is 1.
		h := newHarness(t)
		a, _ := h.create(3, "gpu", typeDocker)
		b, _ := h.create(4, "gpu", typeDocker)
		c, _ := h.create(3, "gpu", typeDocker)
		if a != 1 || b != 2 || c != 3 {
			t.Fatalf("ids %d %d %d", a, b, c)
		}
	})

	t.Run("Equal numbers in two teams grant nothing", func(t *testing.T) {
		h := newHarness(t)
		h.create(3, "gpu", typeDocker)
		for _, op := range []string{"get", "reissue", "delete"} {
			if r := h.ui(op, 4, map[string]any{"team_id": 4, "executor_id": 1}, nil); r.Status != 404 {
				t.Errorf("team 4 %s of executor 1: %d", op, r.Status)
			}
		}
		if g := h.get(3, 1); g.Status != 200 {
			t.Fatal("team 3 Executor changed")
		}
	})

	t.Run("Concurrent creates get unique increasing numbers", func(t *testing.T) {
		h := newHarness(t)
		var wg sync.WaitGroup
		var mu sync.Mutex
		var got []int64
		for i := 0; i < 20; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				r := h.ui("create", 3, map[string]any{"team_id": 3}, map[string]any{"execution_tag": "gpu", "executor_type": typeDocker})
				if r.Status == 201 {
					mu.Lock()
					got = append(got, num(r.Body["executor_id"]))
					mu.Unlock()
				}
			}()
		}
		wg.Wait()
		seen := map[int64]bool{}
		for _, id := range got {
			if seen[id] || id < 1 {
				t.Fatalf("duplicate or invalid id %d in %v", id, got)
			}
			seen[id] = true
		}
		if len(got) != 20 {
			t.Fatalf("%d creates succeeded", len(got))
		}
		// A rolled-back create leaves a gap; the number is not reused.
		h.store.FailNextCommit = errors.New("rollback")
		h.ui("create", 3, map[string]any{"team_id": 3}, map[string]any{"execution_tag": "gpu", "executor_type": typeDocker})
		id, _ := h.create(3, "gpu", typeDocker)
		if id != 22 {
			t.Fatalf("id after rollback = %d, want 22", id)
		}
	})

	t.Run("Rolled-back transaction neither publishes nor reuses its sequence", func(t *testing.T) {
		h := newHarness(t)
		h.store.SetNextPublisherSeq(77)
		h.store.FailNextCommit = errors.New("rollback")
		h.ui("create", 3, map[string]any{"team_id": 3}, map[string]any{"execution_tag": "gpu", "executor_type": typeDocker})
		h.create(3, "gpu", typeDocker)
		h.relay()
		for _, m := range h.broker.Published {
			if m.Header.Get("Nats-Msg-Id") == "executor-registry-77" {
				t.Fatal("rolled-back publisher_seq 77 published")
			}
		}
		for _, m := range h.outbox() {
			if m.Seq <= 77 {
				t.Fatalf("seq %d reused", m.Seq)
			}
		}
		if len(h.outbox()) == 0 {
			t.Fatal("nothing committed")
		}
	})
}
