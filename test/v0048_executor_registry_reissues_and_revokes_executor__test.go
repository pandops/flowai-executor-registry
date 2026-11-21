package test

import (
	"errors"
	"fmt"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/pandops/flowai-executor-registry/internal/broker"
)

// Requirement: executor-registry reissues and revokes Executor JWTs.

func userPub(t *testing.T, token string) string {
	t.Helper()
	return decodeUser(t, token).Subject
}

func TestReissueAndRevoke(t *testing.T) {
	t.Run("Reissue replaces the JWT", func(t *testing.T) {
		h := newHarness(t)
		_, old := h.create(3, "gpu", typeDocker)
		before := h.get(3, 1)
		h.advance(60e9)
		r := h.reissue(3, 1)
		if r.Status != 200 {
			t.Fatalf("reissue = %d %s", r.Status, r.Raw)
		}
		h.validate("ui.reissue.reply.schema.json", r.Raw)
		tok, _ := r.Body["token"].(string)
		if tok == "" || tok == old || r.Body["token_fingerprint"] == before.Body["token_fingerprint"] || r.Body["token_issued_at"] == before.Body["token_issued_at"] {
			t.Fatalf("reissue did not return a new JWT, fingerprint and time: %s", r.Raw)
		}
		after := h.get(3, 1)
		if after.Body["token_fingerprint"] != r.Body["token_fingerprint"] {
			t.Fatalf("stored fingerprint not updated")
		}
		// The previous JWT is revoked on the live account (connection closed, no reconnect).
		ac := decodeAccount(t, h.broker.AccountJWT)
		if !ac.Revocations.IsRevoked(userPub(t, old), iat(t, old)) {
			t.Fatal("previous JWT is not revoked in the pushed account JWT")
		}
		if ac.Revocations.IsRevoked(userPub(t, tok), iat(t, tok)) {
			t.Fatal("new JWT is revoked")
		}
	})

	t.Run("Revocation push fails", func(t *testing.T) {
		for _, mode := range []string{"refused", "unanswered", "lookup"} {
			for _, op := range []string{"reissue", "delete"} {
				h := newHarness(t)
				_, old := h.create(3, "gpu", typeDocker)
				before := h.get(3, 1)
				switch mode {
				case "refused":
					h.broker.FailAccountUpdate = errors.New("account update refused")
				case "unanswered":
					h.broker.FailAccountUpdate = broker.ErrTimeout
				case "lookup":
					h.broker.FailAccountLookup = broker.ErrNoResponders
				}
				consumers := len(h.broker.Consumers)
				var r reply
				if op == "reissue" {
					r = h.reissue(3, 1)
				} else {
					r = h.remove(3, 1)
				}
				name := mode + "/" + op
				if r.Status != 503 || r.code() != "service_unavailable" {
					t.Errorf("%s: %d %s", name, r.Status, r.Raw)
				}
				if _, has := r.Body["token"]; has {
					t.Errorf("%s: reply carries a token", name)
				}
				after := h.get(3, 1)
				if after.Status != 200 || after.Body["token_fingerprint"] != before.Body["token_fingerprint"] {
					t.Errorf("%s: Executor or its token changed: %s", name, after.Raw)
				}
				if len(h.broker.Consumers) != consumers || len(h.events("executor_removed")) != 0 {
					t.Errorf("%s: consumers or executor_removed changed", name)
				}
				if ac := decodeAccount(t, h.broker.AccountJWT); ac.Revocations.IsRevoked(userPub(t, old), iat(t, old)) {
					t.Errorf("%s: previous JWT revoked", name)
				}
				pushes, lookups := len(h.broker.AccountPushes), h.broker.AccountLookups
				// Nothing retries the push automatically.
				h.advance(10 * 60e9)
				h.sweep()
				h.relay()
				if len(h.broker.AccountPushes) != pushes || h.broker.AccountLookups != lookups {
					t.Errorf("%s: push retried automatically", name)
				}
				// The operator repeats the request and it succeeds.
				h.broker.FailAccountUpdate, h.broker.FailAccountLookup = nil, nil
				if op == "reissue" {
					r = h.reissue(3, 1)
				} else {
					r = h.remove(3, 1)
				}
				if r.Status >= 300 {
					t.Errorf("%s: retry = %d %s", name, r.Status, r.Raw)
				}
			}
		}
	})

	t.Run("Reissue keeps the Executor's identity and consumers", func(t *testing.T) {
		h := newHarness(t)
		_, old := h.create(3, "gpu", typeDocker)
		h.heartbeat(1, hb{Claims: []map[string]any{claim(1010, 1, 1, "running")}, MaxParallel: 2, Slots: 1, Interval: 15, Miss: 3, Wait: 600, IP: "192.0.2.10"})
		consumers := fmt.Sprint(h.broker.Consumers)
		r := h.reissue(3, 1)
		tok, _ := r.Body["token"].(string)
		a, b := decodeUser(t, old), decodeUser(t, tok)
		if !reflect.DeepEqual(a.Permissions, b.Permissions) || a.Name != b.Name {
			t.Fatalf("permissions or identity changed:\n%+v\n%+v", a.Permissions, b.Permissions)
		}
		if fmt.Sprint(h.broker.Consumers) != consumers || len(h.broker.Consumers) != 1 {
			t.Fatalf("consumers changed by reissue: %v", h.broker.Consumers)
		}
		for _, m := range h.outbox() {
			if m.Subject != "executor-registry.audit.executor_created.pub" && m.Subject != "executor-registry.audit.executor_token_reissued.pub" &&
				m.Subject != "executor-registry.event.executor_registered.pub" && m.Subject != "executor-registry.event.executor_status.pub" &&
				m.Subject != "executor-registry.event.ip_changed.pub" {
				t.Errorf("unexpected message %s", m.Subject)
			}
		}
		// executor-registry changes no claim: it sent nothing to task-manager except heartbeat claim queries.
		for _, q := range h.broker.Requests {
			if q.Subject != subjTeamGet && q.Subject != subjExecutorClaims {
				t.Errorf("unexpected request %s", q.Subject)
			}
		}
	})

	t.Run("Reissued Executor restarts without continuing agents", func(t *testing.T) {
		h := newHarness(t)
		h.create(3, "gpu", typeDocker)
		h.heartbeat(1, hb{Claims: []map[string]any{claim(1010, 1, 1, "running")}, MaxParallel: 2, Slots: 1, Interval: 15, Miss: 3, Wait: 600, IP: "192.0.2.10"})
		if r := h.reissue(3, 1); r.Status != 200 {
			t.Fatalf("reissue = %d", r.Status)
		}
		// Restarted Executor with the new JWT: same executor_id, heartbeat omits the claim.
		h.advance(20e9)
		h.claimsHeld = func(map[string]any) []map[string]any { return nil }
		r := h.heartbeat(1, defaultHB())
		if r.Status != 200 || r.Body["claims_known"] != true || len(r.Body["claims"].([]any)) != 0 {
			t.Fatalf("heartbeat after reissue = %d %s", r.Status, r.Raw)
		}
		reqs := h.broker.RequestsTo(subjExecutorClaims)
		last := reqs[len(reqs)-1]
		if string(last.Body) == "" || containsClaim(last.Body, 1010) {
			t.Fatalf("executor-claims request should forward the reported (empty) claims: %s", last.Body)
		}
		for _, m := range h.outbox() {
			if m.Subject == "executor-registry.event.released.pub" {
				t.Fatal("executor-registry released a claim itself")
			}
		}
	})

	t.Run("Revocation re-signs the account JWT with the dedicated operator key", func(t *testing.T) {
		h := newHarness(t)
		for i := 0; i < 7; i++ {
			h.create(3, "gpu", typeDocker)
		}
		before := decodeAccount(t, h.broker.AccountJWT)
		// Revoke Executor 6 first so that a second revocation must keep it.
		h.reissue(3, 6)
		r6 := h.broker.AccountJWT
		h.remove(3, 7)
		if len(h.broker.AccountPushes) != 2 {
			t.Fatalf("pushes = %d, want 2", len(h.broker.AccountPushes))
		}
		ac := decodeAccount(t, h.broker.AccountJWT)
		opSig, _ := h.keys.operatorSigning.PublicKey()
		if ac.Issuer != opSig {
			t.Fatalf("account JWT issuer %s, want operator signing key %s", ac.Issuer, opSig)
		}
		if ac.Subject != h.keys.accountPub {
			t.Fatalf("account subject changed")
		}
		if len(ac.Revocations) != 2 {
			t.Fatalf("revocations = %v, want Executor 6's old key and Executor 7's key", ac.Revocations)
		}
		if decodeAccount(t, r6).Revocations == nil {
			t.Fatal("first push has no revocation")
		}
		// Every other claim unchanged.
		ac.Revocations, before.Revocations = nil, nil
		if !reflect.DeepEqual(ac.Account, before.Account) || ac.Name != before.Name || ac.Expires != before.Expires {
			t.Fatalf("other account claims changed:\n%+v\n%+v", ac.Account, before.Account)
		}
	})

	t.Run("Delete revokes the JWT", func(t *testing.T) {
		h := newHarness(t)
		_, tok := h.create(3, "gpu", typeDocker)
		if r := h.remove(3, 1); r.Status != 204 || len(r.Raw) != 0 {
			t.Fatalf("delete = %d %q", r.Status, r.Raw)
		}
		if l := h.ui("list", 3, map[string]any{"team_id": 3}, nil); len(l.Body["items"].([]any)) != 0 {
			t.Fatalf("deleted Executor still listed: %s", l.Raw)
		}
		ac := decodeAccount(t, h.broker.AccountJWT)
		if !ac.Revocations.IsRevoked(userPub(t, tok), iat(t, tok)) {
			t.Fatal("deleted Executor's JWT not revoked")
		}
	})

	t.Run("Foreign Executor cannot be reissued or deleted", func(t *testing.T) {
		h := newHarness(t)
		for i := 0; i < 8; i++ {
			h.create(3, "gpu", typeDocker)
		}
		h.create(4, "gpu", typeDocker) // executor 9 of team 4
		unknown := h.reissue(3, 77)
		for _, r := range []reply{h.reissue(3, 9), h.remove(3, 9)} {
			if r.Status != 404 || r.code() != "executor_unknown_or_unavailable" || r.Body["message"] != unknown.Body["message"] {
				t.Fatalf("foreign = %d %s, unknown = %s", r.Status, r.Raw, unknown.Raw)
			}
		}
		if g := h.get(4, 9); g.Status != 200 {
			t.Fatalf("team 4 Executor changed: %s", g.Raw)
		}
		if len(h.broker.AccountPushes) != 0 || len(h.events("executor_removed")) != 0 {
			t.Fatal("foreign request revoked or removed something")
		}
	})
}

func containsClaim(body []byte, taskID int64) bool {
	return strings.Contains(string(body), fmt.Sprintf(`"task_id":%d`, taskID))
}

func iat(t *testing.T, token string) time.Time {
	t.Helper()
	return time.Unix(decodeUser(t, token).IssuedAt, 0)
}
