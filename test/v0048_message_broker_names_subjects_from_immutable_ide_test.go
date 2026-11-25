package test

import (
	"strings"
	"testing"
	"time"
)

// Requirement: message-broker names subjects from immutable identifiers.

func TestSubjectNames(t *testing.T) {
	t.Run("Identifier cannot form a subject token", func(t *testing.T) {
		for _, tag := range []string{"gpu.large", "gpu*", "gpu>", "a b"} {
			h := newHarness(t)
			r := h.ui("create", 3, map[string]any{"team_id": 3}, map[string]any{"execution_tag": tag, "executor_type": typeDocker})
			if r.Status != 400 || r.code() != "invalid_request" {
				t.Errorf("tag %q: %d %s", tag, r.Status, r.Raw)
			}
			assertNothingCreated(t, h)
		}
	})

	t.Run("Team appears as a subject token only in the task queue and Source intake", func(t *testing.T) {
		h := newHarness(t)
		for i := 0; i < 6; i++ {
			h.create(4, "cpu", typeDocker)
		}
		h.create(3, "gpu", typeDocker)
		h.heartbeat(7, defaultHB())
		h.advance(11 * time.Minute)
		h.sweep()
		h.heartbeat(7, ipHB("10.0.0.9"))
		h.reissue(3, 7)
		h.remove(3, 7)
		h.relay()
		var subjects []string
		for _, m := range h.broker.Published {
			subjects = append(subjects, m.Subject)
		}
		for _, q := range h.broker.Requests {
			subjects = append(subjects, q.Subject)
		}
		for _, s := range subjects {
			if strings.HasPrefix(s, "flowai.") || !strings.HasSuffix(s, ".pub") {
				t.Errorf("subject %s", s)
			}
			for _, tok := range strings.Split(s, ".") {
				if tok == "3" || tok == "4" {
					t.Errorf("subject %s carries a team token", s)
				}
			}
		}
		if len(subjects) == 0 {
			t.Fatal("nothing published")
		}
	})
}
