package test

import "testing"

// Requirement: service-platform service stays independent from execution and identity.
// "Image equality across teams grants no authority" and "Flow cannot be
// created from a Source path" are not applicable to executor-registry (it
// stores no image, Flow or model).

func TestIndependentFromExecution(t *testing.T) {
	t.Run("Executor capacity is not decided centrally", func(t *testing.T) {
		h := newHarness(t)
		h.create(3, "gpu", typeDocker)
		b := defaultHB()
		b.MaxParallel, b.Slots = 1, 3 // more slots in use than capacity
		b.Claims = []map[string]any{claim(1, 1, 1, "running"), claim(2, 1, 1, "running"), claim(3, 1, 1, "pending")}
		r := h.heartbeat(1, b)
		if r.Status != 200 || len(r.Body["claims"].([]any)) != 3 {
			t.Fatalf("heartbeat %d %s", r.Status, r.Raw)
		}
		st := h.events("executor_status")
		if num(st[len(st)-1].Body["available_capacity"]) != 0 {
			t.Fatalf("available below zero or wrong: %s", st[len(st)-1].Raw)
		}
		// Nothing is held back or rejected for capacity: the claims request forwards all three.
		req, _ := lastClaimsReq(t, h)
		if len(req.Body.Claims) != 3 {
			t.Fatal("claims dropped for capacity")
		}
	})
}
