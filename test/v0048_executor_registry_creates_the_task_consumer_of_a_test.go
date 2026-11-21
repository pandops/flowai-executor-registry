package test

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"strings"
	"testing"

	"github.com/pandops/flowai-executor-registry/internal/natsjwt"
)

// Requirement: executor-registry creates the task consumer of an Executor's team and tag.

func tagHash(tag string) string {
	sum := sha256.Sum256([]byte(tag))
	return hex.EncodeToString(sum[:])[:16]
}

func TestTaskConsumer(t *testing.T) {
	t.Run("First Executor of a tag creates the task consumer", func(t *testing.T) {
		h := newHarness(t)
		var consumersAtReply int
		id, tok := h.create(3, "gpu", typeDocker)
		consumersAtReply = len(h.broker.Consumers)
		if id != 1 || tok == "" || consumersAtReply != 1 {
			t.Fatalf("id=%d consumers=%d", id, consumersAtReply)
		}
		c := h.broker.Consumers[0]
		if c.Stream != "TASK_MANAGER_TASK" || c.Durable != "tasks-3-"+tagHash("gpu") || c.FilterSubject != "task-manager.task.3.gpu.*.sub" {
			t.Fatalf("consumer = %+v", c)
		}
		if natsjwt.TaskConsumerName(3, "gpu") != c.Durable || natsjwt.TagHash("gpu") != tagHash("gpu") {
			t.Fatalf("name helpers disagree: %s %s", natsjwt.TaskConsumerName(3, "gpu"), natsjwt.TagHash("gpu"))
		}
		for _, cc := range h.broker.Consumers {
			if strings.Contains(cc.Stream, "COMMAND") || strings.HasPrefix(cc.Durable, "commands-") {
				t.Fatalf("command consumer created: %+v", cc)
			}
		}
		// A consumer creation failure fails the create without an Executor or JWT.
		h2 := newHarness(t)
		h2.broker.FailCreate = errors.New("jetstream api error")
		r := h2.ui("create", 3, map[string]any{"team_id": 3}, map[string]any{"execution_tag": "gpu", "executor_type": typeDocker})
		if r.Status != 503 || r.Body["token"] != nil {
			t.Fatalf("create with failing consumer = %d %s", r.Status, r.Raw)
		}
		assertNothingCreated(t, h2)
	})

	t.Run("Second Executor shares the task consumer", func(t *testing.T) {
		h := newHarness(t)
		h.create(3, "gpu", typeDocker)
		h.create(3, "gpu", typeK8s)
		if len(h.broker.Consumers) != 1 {
			t.Fatalf("consumers = %+v", h.broker.Consumers)
		}
		h.create(4, "gpu", typeDocker)
		if len(h.broker.Consumers) != 2 || h.broker.Consumers[1].Durable != "tasks-4-"+tagHash("gpu") {
			t.Fatalf("other team must get its own consumer: %+v", h.broker.Consumers)
		}
	})

	t.Run("Deleting the last Executor keeps queued tasks", func(t *testing.T) {
		h := newHarness(t)
		h.create(3, "gpu", typeDocker)
		h.remove(3, 1)
		if len(h.broker.Consumers) != 1 {
			t.Fatalf("consumers = %+v", h.broker.Consumers)
		}
		// A later Executor of the same tag reuses the kept consumer.
		h.create(3, "gpu", typeDocker)
		if len(h.broker.Consumers) != 1 {
			t.Fatalf("consumer recreated: %+v", h.broker.Consumers)
		}
	})

	t.Run("Deleted Executor's command consumer disappears after the threshold", func(t *testing.T) {
		h := newHarness(t)
		for i := 0; i < 7; i++ {
			h.create(3, "gpu", typeDocker)
		}
		before := len(h.broker.Consumers)
		if r := h.remove(3, 7); r.Status != 204 {
			t.Fatalf("delete = %d", r.Status)
		}
		if len(h.broker.Consumers) != before {
			t.Fatal("delete created a consumer")
		}
		for _, q := range h.broker.Requests {
			if strings.HasPrefix(q.Subject, "$JS.") {
				t.Fatalf("delete sent JetStream API request %s", q.Subject)
			}
		}
	})
}
