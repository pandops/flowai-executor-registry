package test

import (
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/pandops/flowai-executor-registry/internal/broker"
)

// Requirement: service-platform service stores team ownership in normalized form.
// "Child references a foreign-team parent" is not applicable to
// executor-registry: an Executor has no child record of another team.

func TestTeamOwnership(t *testing.T) {
	t.Run("Team-owned parent keeps its team", func(t *testing.T) {
		h := newHarness(t)
		h.create(3, "gpu", typeDocker)
		h.reissue(3, 1)
		h.heartbeat(1, ipHB("10.0.0.1"))
		h.advance(11 * time.Minute)
		h.sweep()
		h.heartbeat(1, ipHB("10.0.0.2"))
		e := h.store.Executors()
		c, _ := h.store.Connection(1)
		if len(e) != 1 || e[0].TeamID != 3 || e[0].Tag != "gpu" || e[0].Type != typeDocker || c.TeamID != 3 {
			t.Fatalf("ownership changed: %+v %+v", e, c)
		}
		for _, m := range h.outbox() {
			if num(m.Body["team_id"]) != 3 || m.Header["team"] != "3" {
				t.Fatalf("%s carries team %v", m.Subject, m.Body["team_id"])
			}
		}

		// Writes that would move Executor 1 to team 4 are refused and
		// change nothing.
		outboxBefore := len(h.outbox())
		for name, r := range map[string]reply{
			"team-4 reissue": h.reissue(4, 1),
			"team-4 delete":  h.remove(4, 1),
			"team-4 get":     h.get(4, 1),
			"params name team 4 under header team 3": h.uiRaw("reissue", uiHeader(3, h.nextRequestID()),
				envelope(map[string]any{"team_id": 4, "executor_id": 1}, nil)),
		} {
			if r.Status < 400 {
				t.Fatalf("%s accepted: %d %s", name, r.Status, r.Raw)
			}
		}
		moved := ipHB("10.0.0.3")
		body := moved.body()
		body["team_id"] = 4
		h.handle("executor-registry.executor.1.heartbeat.sub", broker.Header{"team": {"4"}, "request_id": {strconv.FormatInt(h.nextRequestID(), 10)}},
			envelope(nil, body))
		e = h.store.Executors()
		c, _ = h.store.Connection(1)
		if len(e) != 1 || e[0].TeamID != 3 || c.TeamID != 3 {
			t.Fatalf("a foreign-team write moved Executor 1: %+v %+v", e, c)
		}
		// Only rejection audits of the acting team may follow; no event
		// describes Executor 1 under another team.
		for _, m := range h.outbox()[outboxBefore:] {
			if strings.Contains(m.Subject, ".audit.") && m.Body["outcome"] == "rejected" {
				continue
			}
			if num(m.Body["team_id"]) != 3 || m.Header["team"] != "3" {
				t.Fatalf("%s carries team %v after a foreign-team write", m.Subject, m.Body["team_id"])
			}
		}
	})

	t.Run("Display metadata never authorizes", func(t *testing.T) {
		h := newHarness(t)
		h.create(4, "gpu", typeDocker)
		hd := uiHeader(3, 7)
		hd["team_name"] = []string{"team-4"}
		if r := h.uiRaw("get", hd, envelope(map[string]any{"team_id": 3, "executor_id": 1}, nil)); r.Status != 404 {
			t.Fatalf("team_name granted access: %d", r.Status)
		}
	})
}
