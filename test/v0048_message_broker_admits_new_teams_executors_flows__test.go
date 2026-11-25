package test

import (
	"testing"
)

// Requirement: message-broker admits new teams, Executors, Flows and Sources without a restart.

func TestAdmitWithoutRestart(t *testing.T) {
	t.Run("Executor is created without a restart", func(t *testing.T) {
		h := newHarness(t)
		for i := 0; i < 11; i++ {
			h.create(4, "cpu", typeDocker)
		}
		svc := h.svc
		id, tok := h.create(3, "gpu", typeDocker)
		if id != 12 || tok == "" {
			t.Fatalf("create %d", id)
		}
		// The JWT is signed by the account signing key the static account JWT already lists.
		uc := decodeUser(t, tok)
		if !decodeAccount(t, h.broker.AccountJWT).SigningKeys.Contains(uc.Issuer) {
			t.Fatal("JWT needs an account change to be accepted")
		}
		// The task consumer was created through the JetStream API; no account push.
		if len(h.broker.AccountPushes) != 0 {
			t.Fatal("create pushed an account JWT")
		}
		found := false
		for _, c := range h.broker.Consumers {
			found = found || c.Durable == "tasks-3-"+tagHash("gpu")
		}
		if !found {
			t.Fatal("task consumer not created at runtime")
		}
		if len(h.eventsFor("executor_registered", 12)) != 1 {
			t.Fatal("no executor_registered for the new Executor")
		}
		// The same running service instance serves the new Executor's heartbeat.
		if h.svc != svc {
			t.Fatal("service rebuilt")
		}
		if r := h.heartbeat(12, defaultHB()); r.Status != 200 {
			t.Fatalf("heartbeat of new Executor = %d", r.Status)
		}
	})

	t.Run("Executor delete and reissue revoke live", func(t *testing.T) {
		h := newHarness(t)
		for i := 0; i < 8; i++ {
			h.create(3, "gpu", typeDocker)
		}
		svc := h.svc
		consumers := len(h.broker.Consumers)
		if r := h.reissue(3, 7); r.Status != 200 {
			t.Fatalf("reissue %d", r.Status)
		}
		if r := h.remove(3, 8); r.Status != 204 {
			t.Fatalf("delete %d", r.Status)
		}
		if len(h.broker.AccountPushes) != 2 || len(h.broker.Consumers) != consumers || h.svc != svc {
			t.Fatalf("pushes %d consumers %d", len(h.broker.AccountPushes), len(h.broker.Consumers))
		}
		if r := h.heartbeat(7, defaultHB()); r.Status != 200 {
			t.Fatalf("reissued Executor heartbeat %d", r.Status)
		}
	})
}
