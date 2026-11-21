package test

import (
	"testing"
)

// Requirement: service-platform service gets foreign data only from the broker.
// executor-registry keeps no projection of another service's data and
// consumes no stream: it asks team-registry live on
// team-registry.service.get.pub. "Projection ignores a replayed event" is
// therefore not applicable to executor-registry.

func TestForeignDataFromBroker(t *testing.T) {
	t.Run("Team archive reaches a projection", func(t *testing.T) {
		h := newHarness(t)
		h.create(3, "gpu", typeDocker)
		h.teams[3] = team{archived: true}
		r := h.ui("create", 3, map[string]any{"team_id": 3}, map[string]any{"execution_tag": "gpu", "executor_type": typeDocker})
		if r.Status < 400 || r.Status >= 500 {
			t.Fatalf("create after archive = %d %s", r.Status, r.Raw)
		}
		if len(h.store.Executors()) != 1 {
			t.Fatal("Executor created for an archived team")
		}
		reqs := h.broker.RequestsTo(subjTeamGet)
		if len(reqs) != 2 || reqs[1].Header.Get("team") != "3" || reqs[1].Header.Get("request_id") == "" {
			t.Fatalf("team checks %v", reqs)
		}
		if string(reqs[1].Body) != `{"params":{"team_id":3}}` {
			t.Fatalf("team check body %s", reqs[1].Body)
		}
	})
}
