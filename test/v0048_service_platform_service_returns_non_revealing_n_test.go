package test

import "testing"

// Requirement: service-platform service returns non-revealing not-found for foreign resources.

func TestNonRevealingNotFound(t *testing.T) {
	t.Run("Operator reads a foreign task", func(t *testing.T) {
		// executor-registry form: a foreign Executor reads like an unknown one.
		h := newHarness(t)
		h.create(4, "gpu", typeDocker)
		f, u := h.get(3, 1), h.get(3, 2)
		if f.Status != 404 || string(f.Raw) != string(u.Raw) && (f.code() != u.code() || f.Body["message"] != u.Body["message"]) {
			t.Fatalf("foreign %s unknown %s", f.Raw, u.Raw)
		}
	})

	t.Run("Operator writes a foreign environment", func(t *testing.T) {
		// executor-registry form: a foreign reissue or delete changes nothing and revokes nothing.
		h := newHarness(t)
		h.create(4, "gpu", typeDocker)
		before := h.get(4, 1)
		for _, r := range []reply{h.reissue(3, 1), h.remove(3, 1)} {
			if r.Status != 404 || r.code() != "executor_unknown_or_unavailable" {
				t.Fatalf("foreign write %d %s", r.Status, r.Raw)
			}
		}
		if after := h.get(4, 1); after.Body["token_fingerprint"] != before.Body["token_fingerprint"] {
			t.Fatal("foreign Executor changed")
		}
		if h.broker.AccountLookups != 0 || len(h.broker.AccountPushes) != 0 {
			t.Fatal("foreign write touched the account")
		}
	})
}
