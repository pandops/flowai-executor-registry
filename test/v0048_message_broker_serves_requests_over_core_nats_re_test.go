package test

import (
	"fmt"
	"testing"

	"github.com/pandops/flowai-executor-registry/internal/app"
)

// Requirement: message-broker serves requests over core NATS request/reply.

func TestRequestReply(t *testing.T) {
	t.Run("Executor heartbeat is served by executor-registry", func(t *testing.T) {
		want := []string{
			"executor-registry.executor.*.heartbeat.sub",
			"executor-registry.service.executor-type.sub",
			"executor-registry.service.team-tags.sub",
			"executor-registry.ui.create.sub",
			"executor-registry.ui.delete.sub",
			"executor-registry.ui.get.sub",
			"executor-registry.ui.list-execution-tags.sub",
			"executor-registry.ui.list.sub",
			"executor-registry.ui.reissue.sub",
		}
		if got := sortedStrings(app.Subscriptions()); fmt.Sprint(got) != fmt.Sprint(want) {
			t.Fatalf("subscriptions %v\nwant %v", got, want)
		}
		h := newHarness(t)
		for i := 0; i < 7; i++ {
			h.create(3, "gpu", typeDocker)
		}
		r := h.heartbeat(7, defaultHB())
		if r.Status != 200 || !r.OK {
			t.Fatalf("heartbeat = %d %s", r.Status, r.Raw)
		}
		// To compose the reply executor-registry requests task-manager on the .pub subject.
		if n := len(h.broker.RequestsTo("task-manager.service.executor-claims.pub")); n != 1 {
			t.Fatalf("executor-claims requests = %d", n)
		}
		for _, q := range h.broker.Requests {
			if q.Subject[len(q.Subject)-4:] != ".pub" {
				t.Fatalf("request on %s", q.Subject)
			}
		}
		// An unknown operation or another service's subject gets no answer.
		for _, s := range []string{"executor-registry.ui._test.sub", "executor-registry.ui.purge.sub", "task-manager.service.executor-claims.sub", "executor-registry.executor.7.claim-token.sub"} {
			if rr := h.handle(s, uiHeader(3, 1), envelope(nil, nil)); rr.OK && rr.Status < 400 {
				t.Errorf("%s answered %d", s, rr.Status)
			}
		}
	})
}
