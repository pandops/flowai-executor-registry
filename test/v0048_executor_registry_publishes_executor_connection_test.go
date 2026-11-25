package test

import (
	"testing"
	"time"
)

// Requirement: executor-registry publishes Executor connection events only on change.

func TestConnectionEventsOnChange(t *testing.T) {
	t.Run("Unchanged heartbeat publishes no status", func(t *testing.T) {
		h := newHarness(t)
		h.create(3, "gpu", typeDocker)
		b := defaultHB()
		b.Claims, b.Slots = []map[string]any{claim(1010, 1, 1, "running")}, 1
		h.heartbeat(1, b)
		n := len(h.outbox())
		for i := 0; i < 10; i++ {
			h.advance(15 * time.Second)
			h.heartbeat(1, b)
			h.sweep()
		}
		if len(h.outbox()) != n {
			t.Fatalf("unchanged heartbeats published %v", h.outbox()[n:])
		}
		b.Claims = append(b.Claims, claim(1011, 1, 1, "created"))
		h.heartbeat(1, b)
		after := h.outbox()[n:]
		if len(after) != 1 || after[0].Subject != "executor-registry.event.executor_status.pub" || num(after[0].Body["held_claims"]) != 2 {
			t.Fatalf("added claim published %v", after)
		}
		// Capacity change publishes too.
		b.Slots = 2
		h.heartbeat(1, b)
		st := h.events("executor_status")
		if num(st[len(st)-1].Body["available_capacity"]) != 0 || len(st) != 3 {
			t.Fatalf("capacity change status %v", st)
		}
		h.checkOutbox()
	})

	t.Run("Connection events carry the executor-registry publisher", func(t *testing.T) {
		h := newHarness(t)
		for i := 0; i < 7; i++ {
			h.create(3, "gpu", typeDocker)
		}
		h.heartbeat(7, defaultHB())
		h.store.SetNextPublisherSeq(310)
		h.advance(45 * time.Second)
		h.sweep()
		ev := h.eventsFor("connection_lost", 7)
		if len(ev) != 1 || ev[0].Seq != 310 || num(ev[0].Body["publisher_seq"]) != 310 || ev[0].Body["publisher"] != "executor-registry" || num(ev[0].Body["team_id"]) != 3 || ev[0].Body["kind"] != "connection_lost" {
			t.Fatalf("connection_lost %v", ev)
		}
		h.relay()
		var found bool
		for _, m := range h.broker.Published {
			if m.Subject == "executor-registry.event.connection_lost.pub" {
				found = m.Header.Get("Nats-Msg-Id") == "executor-registry-310" && m.Header.Get("team") == "3"
			}
		}
		if !found {
			t.Fatalf("published %v", h.broker.Published)
		}
		h.checkOutbox()
	})

	t.Run("Unknown claims keep the held-claims count", func(t *testing.T) {
		h := newHarness(t)
		h.create(3, "gpu", typeDocker)
		b := defaultHB()
		b.Claims = []map[string]any{claim(1010, 1, 1, "running"), claim(1011, 1, 1, "running")}
		h.heartbeat(1, b)
		c, _ := h.store.Connection(1)
		if c.HeldClaims != 2 {
			t.Fatalf("held = %d", c.HeldClaims)
		}
		h.broker.Respond(subjExecutorClaims, nil)
		n := len(h.outbox())
		for i := 0; i < 3; i++ {
			h.advance(15 * time.Second)
			r := h.heartbeat(1, defaultHB())
			if r.Body["claims_known"] != false {
				t.Fatalf("reply %s", r.Raw)
			}
		}
		if len(h.outbox()) != n {
			t.Fatalf("published %v", h.outbox()[n:])
		}
		if c, _ := h.store.Connection(1); c.HeldClaims != 2 {
			t.Fatalf("held = %d", c.HeldClaims)
		}
	})
}
