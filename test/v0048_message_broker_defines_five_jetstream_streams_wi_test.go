package test

import (
	"regexp"
	"strings"
	"testing"

	"github.com/pandops/flowai-executor-registry/internal/natsjwt"
)

// Requirement: message-broker defines five JetStream streams (executor-registry part:
// consumer names derived from identifiers and the tag hash).

var natsName = regexp.MustCompile(`^[A-Za-z0-9_-]{1,255}$`)

func TestConsumerNames(t *testing.T) {
	t.Run("Consumer names are derived from identifiers and the tag hash", func(t *testing.T) {
		h := newHarness(t)
		for i := 0; i < 6; i++ {
			h.create(4, "x", typeDocker)
		}
		id7, tok7 := h.create(3, "GPU_large-1", typeDocker)
		id8, tok8 := h.create(3, "GPU_large-1", typeK8s)
		want := "tasks-3-" + tagHash("GPU_large-1")
		if id7 != 7 || id8 != 8 {
			t.Fatalf("ids %d %d", id7, id8)
		}
		var mine []string
		for _, c := range h.broker.Consumers {
			if !natsName.MatchString(c.Durable) {
				t.Errorf("consumer name %q is not a valid NATS name", c.Durable)
			}
			if c.FilterSubject == "task-manager.task.3.GPU_large-1.*.sub" {
				mine = append(mine, c.Durable)
			}
		}
		if len(mine) != 1 || mine[0] != want || natsjwt.TaskConsumerName(3, "GPU_large-1") != want {
			t.Fatalf("team 3 consumers %v, want [%s]", mine, want)
		}
		// Executor 8 reuses the consumer: executor-registry creates exactly one
		// consumer for team 3, the task consumer, and no command consumer for
		// either Executor (they create commands-7 and commands-8 themselves).
		var team3 []string
		for _, c := range h.broker.Consumers {
			if strings.HasPrefix(c.Durable, "commands-") || c.Stream != "TASK_MANAGER_TASK" ||
				strings.HasPrefix(c.FilterSubject, "task-manager.command.") {
				t.Errorf("executor-registry created a non-task consumer: %+v", c)
			}
			if strings.HasPrefix(c.Durable, "tasks-3-") || strings.HasPrefix(c.FilterSubject, "task-manager.task.3.") {
				team3 = append(team3, c.Durable+" "+c.Stream+" "+c.FilterSubject)
			}
		}
		if wantTeam3 := want + " TASK_MANAGER_TASK task-manager.task.3.GPU_large-1.*.sub"; len(team3) != 1 || team3[0] != wantTeam3 {
			t.Fatalf("team 3 consumers created = %v, want exactly [%s]", team3, wantTeam3)
		}
		for _, r := range h.broker.Requests {
			if strings.HasPrefix(r.Subject, "$JS.API.CONSUMER.") {
				t.Errorf("executor-registry sent a consumer API request %s", r.Subject)
			}
		}
		// Both JWTs pull from the same task consumer and create only their own command consumer.
		for id, tok := range map[int64]string{7: tok7, 8: tok8} {
			uc := decodeUser(t, tok)
			if !allowed(uc.Pub.Allow, "$JS.API.CONSUMER.MSG.NEXT.TASK_MANAGER_TASK."+want) {
				t.Errorf("executor %d cannot pull %s", id, want)
			}
			own := "commands-" + map[int64]string{7: "7", 8: "8"}[id]
			if !allowed(uc.Pub.Allow, "$JS.API.CONSUMER.CREATE.TASK_MANAGER_COMMAND."+own+".task-manager.command."+own[9:]+".sub") {
				t.Errorf("executor %d cannot create %s", id, own)
			}
			if !natsName.MatchString(own) {
				t.Errorf("%s invalid", own)
			}
		}
		// Bounded length for the longest tag.
		long := natsjwt.TaskConsumerName(9223372036854775807, strings.Repeat("Z", 128))
		if len(long) > 64 || !natsName.MatchString(long) {
			t.Fatalf("long name %q", long)
		}
	})
}
