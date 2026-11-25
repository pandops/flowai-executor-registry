package test

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strings"
	"testing"

	"github.com/pandops/flowai-executor-registry/internal/natsjwt"
)

// Requirement: executor-registry issues the Executor NATS JWT once.

func TestIssueExecutorJWT(t *testing.T) {
	t.Run("Create returns the JWT once", func(t *testing.T) {
		h := newHarness(t)
		r := h.ui("create", 3, map[string]any{"team_id": 3}, map[string]any{"execution_tag": "gpu", "executor_type": typeDocker})
		if r.Status != 201 {
			t.Fatalf("create = %d %s", r.Status, r.Raw)
		}
		tok, _ := r.Body["token"].(string)
		if tok == "" || r.Body["token_fingerprint"] == nil || r.Body["token_issued_at"] == nil || num(r.Body["executor_id"]) != 1 {
			t.Fatalf("create reply lacks executor_id/token/fingerprint/issue time: %s", r.Raw)
		}
		sum := sha256.Sum256([]byte(tok))
		if r.Body["token_fingerprint"] != hex.EncodeToString(sum[:]) {
			t.Fatalf("fingerprint %v is not SHA-256 of the JWT", r.Body["token_fingerprint"])
		}
		g := h.get(3, 1)
		l := h.ui("list", 3, map[string]any{"team_id": 3}, nil)
		for name, rep := range map[string]reply{"get": g, "list": l} {
			if rep.Status != 200 {
				t.Fatalf("%s = %d %s", name, rep.Status, rep.Raw)
			}
			if strings.Contains(string(rep.Raw), tok) || strings.Contains(string(rep.Raw), `"token"`) {
				t.Fatalf("%s returns the JWT: %s", name, rep.Raw)
			}
			if !strings.Contains(string(rep.Raw), hex.EncodeToString(sum[:])) || !strings.Contains(string(rep.Raw), "token_issued_at") {
				t.Fatalf("%s lacks fingerprint or issue time: %s", name, rep.Raw)
			}
		}
	})

	t.Run("JWT pins the Executor's subjects", func(t *testing.T) {
		h := newHarness(t)
		for i := 0; i < 6; i++ {
			h.create(3, "cpu", typeDocker)
		}
		id, tok := h.create(3, "gpu", typeDocker)
		if id != 7 {
			t.Fatalf("id = %d, want 7", id)
		}
		uc := decodeUser(t, tok)
		th := natsjwt.TagHash("gpu")
		task := "tasks-3-" + th
		// nats-server checks publish permissions after the account subject
		// mapping, so the Executor's .pub subjects are granted as their mapped
		// .sub subjects (deviation D7, as the deployment's static users).
		wantPub := []string{
			"executor.event.7.*.sub",
			"*.executor.7.*.sub",
			"$JS.API.CONSUMER.MSG.NEXT.TASK_MANAGER_TASK." + task,
			"$JS.ACK.TASK_MANAGER_TASK." + task + ".>",
			"$JS.API.CONSUMER.CREATE.TASK_MANAGER_COMMAND.commands-7.task-manager.command.7.sub",
			"$JS.API.CONSUMER.MSG.NEXT.TASK_MANAGER_COMMAND.commands-7",
			"$JS.ACK.TASK_MANAGER_COMMAND.commands-7.>",
		}
		if got := sortedStrings(uc.Pub.Allow); fmt.Sprint(got) != fmt.Sprint(sortedStrings(wantPub)) {
			t.Fatalf("pub allow = %v\nwant %v", got, sortedStrings(wantPub))
		}
		if got := sortedStrings(uc.Sub.Allow); fmt.Sprint(got) != fmt.Sprint([]string{"_INBOX.executor-7.>"}) {
			t.Fatalf("sub allow = %v", got)
		}
		if len(uc.Pub.Deny) != 0 || len(uc.Sub.Deny) != 0 {
			t.Fatalf("unexpected deny lists %v %v", uc.Pub.Deny, uc.Sub.Deny)
		}
		for _, s := range []string{
			"executor.event.8.created.pub", "task-manager.task.3.gpu.12.pub", "task-manager.executor.8.claim-token.pub",
			"executor-registry.ui.create.pub", "$JS.API.CONSUMER.CREATE.TASK_MANAGER_COMMAND.commands-8.task-manager.command.8.sub",
			"$JS.API.CONSUMER.CREATE.TASK_MANAGER_COMMAND.commands-7.task-manager.command.>", "$JS.API.STREAM.LIST",
			"$JS.API.CONSUMER.MSG.NEXT.TASK_MANAGER_TASK.tasks-4-" + th,
		} {
			if allowed(uc.Pub.Allow, afterMapping(s)) {
				t.Errorf("JWT allows publishing %s", s)
			}
		}
		for _, s := range []string{"executor.event.7.created.pub", "executor-registry.executor.7.heartbeat.pub", "task-manager.executor.7.claim-token.pub"} {
			if !allowed(uc.Pub.Allow, afterMapping(s)) {
				t.Errorf("JWT does not allow %s", s)
			}
		}
		for _, p := range uc.Pub.Allow {
			if strings.HasSuffix(p, ".pub") {
				t.Errorf("grant %s is on an unmapped .pub subject", p)
			}
		}
		for _, s := range []string{"executor-registry.ui.create.pub", "executor-registry.admin.purge.pub", "team-registry.service.get.pub", "executor.event.8.created.pub", "task-manager.event.claimed.pub"} {
			if allowed(uc.Pub.Allow, afterMapping(s)) || allowed(uc.Pub.Allow, s) {
				t.Errorf("JWT allows %s before or after the mapping", s)
			}
		}
		if !uc.BearerToken {
			t.Fatalf("JWT is not a bearer token")
		}
		var ident struct {
			TeamID       int64  `json:"team_id"`
			ExecutorID   int64  `json:"executor_id"`
			Tag          string `json:"tag"`
			ExecutorType string `json:"executor_type"`
		}
		if err := json.Unmarshal([]byte(uc.Name), &ident); err != nil || ident.TeamID != 3 || ident.ExecutorID != 7 || ident.Tag != "gpu" || ident.ExecutorType != typeDocker {
			t.Fatalf("name claim %q does not carry the identity (rule 14): %v", uc.Name, err)
		}
	})

	t.Run("JWT is never stored or logged in plaintext", func(t *testing.T) {
		h := newHarness(t)
		_, tok := h.create(3, "gpu", typeDocker)
		r := h.reissue(3, 1)
		tok2, _ := r.Body["token"].(string)
		h.remove(3, 1)
		_, tok3 := h.create(3, "gpu", typeDocker)
		for _, token := range []string{tok, tok2, tok3} {
			if token == "" {
				t.Fatal("no token issued")
			}
			sig := token[strings.LastIndex(token, ".")+1:]
			for _, m := range h.outbox() {
				if strings.Contains(string(m.Raw), sig) {
					t.Errorf("outbox %s carries the JWT", m.Subject)
				}
			}
			if strings.Contains(h.logs.String(), sig) {
				t.Errorf("log carries the JWT")
			}
		}
		for _, e := range h.store.Executors() {
			if strings.Count(e.UserPublicKey, ".") > 0 || len(e.TokenFingerprint) != 64 || e.TokenIssuedAt.IsZero() || !strings.HasPrefix(e.UserPublicKey, "U") {
				t.Errorf("stored token fields %+v are not public key, fingerprint and time", e)
			}
		}
		for _, s := range []string{string(seed(t, h.keys.accountSigning)), string(seed(t, h.keys.operatorSigning))} {
			if strings.Contains(h.logs.String(), s) {
				t.Error("log carries a signing seed")
			}
			for _, m := range h.outbox() {
				if strings.Contains(string(m.Raw), s) {
					t.Error("outbox carries a signing seed")
				}
			}
		}
	})

	t.Run("Executor JWT is signed by the account signing key", func(t *testing.T) {
		h := newHarness(t)
		_, tok := h.create(3, "gpu", typeDocker)
		uc := decodeUser(t, tok)
		sigPub, _ := h.keys.accountSigning.PublicKey()
		if uc.Issuer != sigPub {
			t.Fatalf("issuer %s, want account signing key %s", uc.Issuer, sigPub)
		}
		if uc.IssuerAccount != h.keys.accountPub {
			t.Fatalf("issuer_account %s, want %s", uc.IssuerAccount, h.keys.accountPub)
		}
		// The signing key must be one the account JWT lists, so the server accepts it.
		if ac := decodeAccount(t, h.broker.AccountJWT); !ac.SigningKeys.Contains(uc.Issuer) {
			t.Fatal("issuer is not an account signing key")
		}
		// The operator identity key is refused as the "operator signing key".
		_, err := natsjwt.NewSigner(natsjwt.Keys{
			AccountPublicKey: h.keys.accountPub, AccountSigningSeed: seed(t, h.keys.accountSigning),
			OperatorSigningSeed: seed(t, h.keys.operatorIdentity), InitialAccountJWT: h.keys.initialAccount,
		})
		if err == nil {
			t.Fatal("signer accepted the operator identity key")
		}
		// An account seed is refused as the operator key, an operator seed as the account key.
		if _, err := natsjwt.NewSigner(natsjwt.Keys{AccountPublicKey: h.keys.accountPub, AccountSigningSeed: seed(t, h.keys.operatorSigning), OperatorSigningSeed: seed(t, h.keys.operatorSigning), InitialAccountJWT: h.keys.initialAccount}); err == nil {
			t.Fatal("signer accepted an operator seed as account signing key")
		}
		if _, err := natsjwt.NewSigner(natsjwt.Keys{AccountPublicKey: h.keys.accountPub, AccountSigningSeed: seed(t, h.keys.accountSigning), OperatorSigningSeed: seed(t, h.keys.accountSigning), InitialAccountJWT: h.keys.initialAccount}); err == nil {
			t.Fatal("signer accepted an account seed as operator signing key")
		}
	})
}

// afterMapping is the subject nats-server checks a publish against: the
// account maps every <...>.pub subject to the same subject ending in .sub.
func afterMapping(subject string) string {
	if strings.HasSuffix(subject, ".pub") {
		return strings.TrimSuffix(subject, ".pub") + ".sub"
	}
	return subject
}
