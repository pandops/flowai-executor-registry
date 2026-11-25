package test

import (
	"strings"
	"testing"
	"time"
)

// Requirement: message-broker never carries secrets in streams.

func TestNoSecretsInStreams(t *testing.T) {
	t.Run("Stream content inspection", func(t *testing.T) {
		h := newHarness(t)
		var tokens []string
		for i := 0; i < 3; i++ {
			_, tok := h.create(3, "gpu", typeDocker)
			tokens = append(tokens, tok)
		}
		r := h.reissue(3, 2)
		tokens = append(tokens, r.Body["token"].(string))
		h.heartbeat(1, defaultHB())
		h.advance(11 * time.Minute)
		h.sweep()
		h.remove(3, 3)
		h.relay()
		secrets := []string{string(seed(t, h.keys.accountSigning)), string(seed(t, h.keys.operatorSigning)), string(h.cursorKey)}
		for _, tok := range tokens {
			secrets = append(secrets, tok[strings.LastIndex(tok, ".")+1:])
		}
		for _, m := range h.broker.Published {
			for _, s := range secrets {
				if strings.Contains(string(m.Data), s) {
					t.Errorf("%s carries a secret", m.Subject)
				}
			}
			if strings.Contains(string(m.Data), "eyJ") {
				t.Errorf("%s carries a JWT-like value", m.Subject)
			}
		}
		if len(h.broker.Published) == 0 {
			t.Fatal("nothing published")
		}
	})

	t.Run("One-time Executor JWT crosses the broker only in a reply", func(t *testing.T) {
		h := newHarness(t)
		r := h.ui("create", 3, map[string]any{"team_id": 3}, map[string]any{"execution_tag": "gpu", "executor_type": typeDocker})
		tok, _ := r.Body["token"].(string)
		if tok == "" || !strings.Contains(string(r.Raw), tok) {
			t.Fatal("reply does not carry the JWT")
		}
		h.relay()
		sig := tok[strings.LastIndex(tok, ".")+1:]
		for _, m := range h.broker.Published {
			if strings.Contains(string(m.Data), sig) {
				t.Errorf("stream message %s carries the JWT", m.Subject)
			}
		}
		for _, q := range h.broker.Requests {
			if strings.Contains(string(q.Body), sig) {
				t.Errorf("request %s carries the JWT", q.Subject)
			}
		}
		if strings.Contains(h.logs.String(), sig) {
			t.Error("log carries the JWT")
		}
	})
}
