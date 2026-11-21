package test

import (
	"os"
	"strings"
	"testing"
	"time"
)

// Requirement: message-broker message payloads follow publisher-owned JSON Schemas.

func TestPublisherSchemas(t *testing.T) {
	t.Run("Publisher message violates its own schema", func(t *testing.T) {
		h := newHarness(t)
		h.create(3, "gpu", typeDocker)
		h.heartbeat(1, defaultHB())
		st := h.events("executor_status")
		if len(st) != 1 {
			t.Fatal("no executor_status")
		}
		// The real message validates; the same message without held_claims does not.
		if err := schemaError(t, "event.executor_status.schema.json", st[0].Raw); err != nil {
			t.Fatalf("published executor_status invalid: %v", err)
		}
		delete(st[0].Body, "held_claims")
		if err := schemaError(t, "event.executor_status.schema.json", mustJSON(st[0].Body)); err == nil {
			t.Fatal("executor_status without held_claims validates")
		}
		bad := map[string]any{"publisher": "executor-registry", "publisher_seq": 1, "team_id": 3, "occurred_at": "2026-10-15T08:58:00.000Z", "kind": "executor_registered", "executor_id": 1, "execution_tag": "gpu", "executor_type": typeDocker, "name": "builder"}
		if err := schemaError(t, "event.executor_registered.schema.json", mustJSON(bad)); err == nil {
			t.Fatal("executor_registered with a name validates")
		}
		// Every message of a full Executor lifecycle validates against its own schema.
		h.advance(time.Minute)
		h.sweep()
		h.advance(10 * time.Minute)
		h.sweep()
		h.heartbeat(1, ipHB("10.0.0.2"))
		h.reissue(3, 1)
		h.remove(3, 1)
		h.reissue(3, 1)
		h.checkOutbox()
		kinds := map[string]bool{}
		for _, m := range h.outbox() {
			kinds[m.Subject] = true
		}
		for _, k := range []string{"executor_registered", "executor_removed", "connection_lost", "executor_lost", "connection_restored", "executor_status", "ip_changed"} {
			if !kinds["executor-registry.event."+k+".pub"] {
				t.Errorf("lifecycle did not publish %s", k)
			}
		}
	})

	t.Run("Consumer contract test reads the publisher schema", func(t *testing.T) {
		// executor-registry consumes no stream; as a requester it reads the
		// responders' reply schemas and accepts their valid replies.
		for _, f := range []struct{ file, example string }{
			{"svc/team-registry/schemas/service.get.reply.schema.json", `{"team_id":3,"team_name":"team-3","default_image":"ghcr.io/flowai/agent@sha256:` + strings.Repeat("a", 64) + `","ingested_at":"2026-09-01T00:00:00.000Z","archived_at":null}`},
			{"svc/task-manager/schemas/service.executor-claims.reply.schema.json", `{"executor_id":7,"claims":[{"task_id":1010,"attempt":1,"claim_seq":1}]}`},
		} {
			found := contractFixturePath(t, f.file)
			s := compileSchema(t, found)
			inst, err := unmarshalJSON(f.example)
			if err != nil {
				t.Fatal(err)
			}
			if err := s.Validate(inst); err != nil {
				t.Fatalf("fake reply used by executor-registry tests does not follow %s: %v", found, err)
			}
		}
		// The test imports no other service's package (checked by the Go import rule and here).
		for _, p := range productionGoFiles(t) {
			data, _ := os.ReadFile(p)
			for _, other := range []string{"svc/team-registry/", "svc/task-manager/", "svc/journal/", "svc/state-registry/"} {
				if strings.Contains(string(data), "github.com/flowai/platform/"+other) ||
					strings.Contains(string(data), "github.com/pandops/flowai-"+strings.TrimSuffix(strings.TrimPrefix(other, "svc/"), "/")) {
					t.Errorf("%s imports %s", p, other)
				}
			}
		}
	})

	t.Run("Schema change selects its consumers", func(t *testing.T) {
		got := changedOwners(t, "svc/executor-registry/schemas/event.executor_status.schema.json")
		for _, want := range []string{`"executor-registry"`, `"journal"`} {
			if !strings.Contains(got, want) {
				t.Fatalf("executor_status schema change selects %s, missing %s", got, want)
			}
		}
		if got := changedOwners(t, "svc/executor-registry/schemas/event.executor_lost.schema.json"); !strings.Contains(got, `"task-manager"`) || !strings.Contains(got, `"journal"`) {
			t.Fatalf("executor_lost schema change selects %s", got)
		}
	})
}
