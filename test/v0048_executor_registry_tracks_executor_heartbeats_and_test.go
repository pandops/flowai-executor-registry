package test

import (
	"testing"
	"time"

	"github.com/pandops/flowai-executor-registry/internal/store"
)

// Requirement: executor-registry tracks Executor heartbeats and the connection view.

func connState(t *testing.T, h *harness, id int64) string {
	t.Helper()
	c, ok := h.store.Connection(id)
	if !ok {
		return ""
	}
	return c.State
}

func TestHeartbeatTracking(t *testing.T) {
	t.Run("Three heartbeats missed", func(t *testing.T) {
		h := newHarness(t)
		h.create(3, "gpu", typeDocker)
		h.heartbeat(1, defaultHB())
		last := h.clock()
		h.advance(44 * time.Second)
		h.sweep()
		if s := connState(t, h, 1); s != store.StateConnected || len(h.events("connection_lost")) != 0 {
			t.Fatalf("lost before 45 s: %s", s)
		}
		h.advance(time.Second)
		h.sweep()
		ev := h.events("connection_lost")
		if s := connState(t, h, 1); s != store.StateConnectionLost || len(ev) != 1 {
			t.Fatalf("state %s, events %d", s, len(ev))
		}
		if ev[0].Body["last_heartbeat_at"] != last.Format("2006-01-02T15:04:05.000Z") {
			t.Fatalf("last_heartbeat_at = %v", ev[0].Body["last_heartbeat_at"])
		}
		h.sweep()
		if len(h.events("connection_lost")) != 1 {
			t.Fatal("connection_lost published twice")
		}
		h.checkOutbox()
	})

	t.Run("View is executor_lost from W expiry until the next heartbeat", func(t *testing.T) {
		h := newHarness(t)
		h.create(3, "gpu", typeDocker)
		h.heartbeat(1, defaultHB())
		h.advance(60 * time.Second)
		h.sweep()
		h.advance(539 * time.Second)
		h.sweep()
		if s := connState(t, h, 1); s != store.StateConnectionLost {
			t.Fatalf("state before W = %s", s)
		}
		h.advance(time.Second) // 600 s after the last heartbeat
		h.sweep()
		if s := connState(t, h, 1); s != store.StateExecutorLost || len(h.events("executor_lost")) != 1 {
			t.Fatalf("state at W = %s", s)
		}
		st := h.events("executor_status")
		if st[len(st)-1].Body["connection_state"] != store.StateExecutorLost {
			t.Fatalf("executor_status after W = %s", st[len(st)-1].Raw)
		}
		h.advance(60 * time.Second)
		h.sweep()
		if s := connState(t, h, 1); s != store.StateExecutorLost || len(h.events("executor_lost")) != 1 {
			t.Fatalf("view left executor_lost without a heartbeat: %s", s)
		}
		h.advance(60 * time.Second)
		r := h.heartbeat(1, defaultHB())
		if r.Body["connection_state"] != store.StateConnected || connState(t, h, 1) != store.StateConnected {
			t.Fatalf("heartbeat reply %s", r.Raw)
		}
		if len(h.events("connection_restored")) != 1 {
			t.Fatal("no connection_restored")
		}
		st = h.events("executor_status")
		if st[len(st)-1].Body["connection_state"] != store.StateConnected {
			t.Fatalf("executor_status after restore = %s", st[len(st)-1].Raw)
		}
		h.checkOutbox()
	})

	t.Run("Interval and miss limit come from the last successful heartbeat", func(t *testing.T) {
		h := newHarness(t)
		h.create(3, "gpu", typeDocker)
		b := defaultHB()
		b.Interval, b.Miss = 30, 5
		h.heartbeat(1, b)
		h.advance(120 * time.Second)
		h.sweep()
		h.advance(29 * time.Second)
		h.sweep()
		if connState(t, h, 1) != store.StateConnected {
			t.Fatal("lost before 150 s")
		}
		h.advance(time.Second)
		h.sweep()
		if connState(t, h, 1) != store.StateConnectionLost {
			t.Fatal("not lost at 150 s")
		}
		c, _ := h.store.Connection(1)
		if c.IntervalSeconds != 30 || c.MissLimit != 5 {
			t.Fatalf("stored timing %+v", c)
		}
	})

	t.Run("Defaults before the first heartbeat", func(t *testing.T) {
		h := newHarness(t)
		h.create(3, "gpu", typeDocker)
		c, _ := h.store.Connection(1)
		if c.State != store.StateNeverConnected || c.IntervalSeconds != 15 || c.MissLimit != 3 || c.WaitSeconds != 600 {
			t.Fatalf("defaults %+v", c)
		}
		h.advance(2 * time.Hour)
		h.sweep()
		if connState(t, h, 1) != store.StateNeverConnected || len(h.events("connection_lost")) != 0 || len(h.events("executor_lost")) != 0 {
			t.Fatal("never-connected Executor changed state")
		}
	})

	t.Run("Heartbeat returns within W", func(t *testing.T) {
		h := newHarness(t)
		h.create(3, "gpu", typeDocker)
		claims := []map[string]any{claim(1010, 1, 1, "running"), claim(1011, 1, 1, "pending")}
		b := defaultHB()
		b.Claims, b.Slots = claims, 2
		h.heartbeat(1, b)
		h.advance(60 * time.Second)
		h.sweep()
		h.advance(120 * time.Second) // 3 minutes after the last successful heartbeat
		r := h.heartbeat(1, b)
		if len(h.events("connection_restored")) != 1 || len(h.events("executor_lost")) != 0 {
			t.Fatal("restore events wrong")
		}
		if r.Body["connection_state"] != "connected" || r.Body["claims_known"] != true || len(r.Body["claims"].([]any)) != 2 {
			t.Fatalf("reply %s", r.Raw)
		}
	})

	t.Run("First heartbeat of a new Executor", func(t *testing.T) {
		h := newHarness(t)
		h.create(3, "gpu", typeDocker)
		r := h.heartbeat(1, hb{MaxParallel: 2, Slots: 0, Interval: 15, Miss: 3, Wait: 600, IP: "192.0.2.10"})
		if r.Status != 200 || r.Body["connection_state"] != "connected" || r.Body["claims_known"] != true || len(r.Body["claims"].([]any)) != 0 {
			t.Fatalf("reply %d %s", r.Status, r.Raw)
		}
		st := h.events("executor_status")
		if len(st) != 1 || st[0].Body["connection_state"] != "connected" || num(st[0].Body["available_capacity"]) != 2 || num(st[0].Body["max_parallel_tasks"]) != 2 || num(st[0].Body["held_claims"]) != 0 {
			t.Fatalf("executor_status %v", st)
		}
		if len(h.events("connection_restored")) != 0 {
			t.Fatal("first heartbeat published connection_restored")
		}
		h.checkOutbox()
	})

	t.Run("W comes from the last successful heartbeat", func(t *testing.T) {
		h := newHarness(t)
		h.create(3, "gpu", typeDocker)
		b := defaultHB()
		h.heartbeat(1, b)
		h.advance(15 * time.Second)
		b.Wait = 240
		h.heartbeat(1, b)
		h.advance(239 * time.Second)
		h.sweep()
		if len(h.events("executor_lost")) != 0 {
			t.Fatal("lost before 4 minutes")
		}
		h.advance(time.Second)
		h.sweep()
		if len(h.events("executor_lost")) != 1 {
			t.Fatal("not lost 4 minutes after the last heartbeat")
		}
	})

	t.Run("Deleted Executor is no longer tracked", func(t *testing.T) {
		h := newHarness(t)
		for i := 0; i < 7; i++ {
			h.create(3, "gpu", typeDocker)
		}
		h.heartbeat(7, defaultHB())
		h.advance(50 * time.Second)
		h.sweep()
		if connState(t, h, 7) != store.StateConnectionLost {
			t.Fatal("not connection_lost")
		}
		h.remove(3, 7)
		n := len(h.outbox())
		h.advance(20 * time.Minute)
		h.sweep()
		for _, m := range h.outbox()[n:] {
			t.Errorf("published %s after delete", m.Subject)
		}
		r := h.heartbeat(7, defaultHB())
		if r.Status != 404 || r.code() != "executor_unknown_or_unavailable" {
			t.Fatalf("heartbeat after delete = %d %s", r.Status, r.Raw)
		}
		if len(h.outbox()) != n {
			t.Fatal("heartbeat after delete published")
		}
		if len(h.broker.RequestsTo(subjExecutorClaims)) != 1 {
			t.Fatal("heartbeat after delete queried task-manager")
		}
	})
}
