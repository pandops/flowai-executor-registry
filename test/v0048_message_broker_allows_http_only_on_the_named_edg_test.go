package test

import (
	"fmt"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/pandops/flowai-executor-registry/internal/server"
)

// Requirement: message-broker allows HTTP only on the named edges.
// "Source system reads its settings on the public edge", "Executor edge
// errors are distinguishable" and "Journal stream edge requires the Gateway
// token" are not applicable to executor-registry (source-controller,
// env-registry and journal edges).

func TestHTTPOnlyProbes(t *testing.T) {
	t.Run("Service offers no internal HTTP API", func(t *testing.T) {
		h := newHarness(t)
		h.create(3, "gpu", typeDocker)
		h.heartbeat(1, defaultHB())
		executors := fmt.Sprintf("%+v", h.store.Executors())
		outbox := len(h.outbox())
		requests, published, consumers, pushes := len(h.broker.Requests), len(h.broker.Published), len(h.broker.Consumers), len(h.broker.AccountPushes)

		// The handler the binary serves, wired with the store and broker state.
		hd := server.Handler(h.store, h.broker.Connected)
		for _, c := range []struct {
			method, path string
			want         int
		}{
			{"GET", "/v1/livez", 200},
			{"GET", "/v1/readyz", 200},
			{"GET", "/v1/executors", 404},
			{"GET", "/v1/executors/1", 404},
			{"PUT", "/v1/executors/1", 404},
			{"DELETE", "/v1/executors/1", 404},
			{"POST", "/ui/v1/teams/3/executors", 404},
			{"GET", "/ui/v1/teams/3/executors", 404},
			{"POST", "/ui/v1/teams/3/executors/1/token", 404},
			{"GET", "/internal/v1/teams/3", 404},
			{"POST", "/v1/heartbeat", 404},
			{"GET", "/v1/_test/decrypt-ops", 404},
			{"GET", "/healthz", 404},
			{"GET", "/metrics", 404},
			{"GET", "/debug/pprof/", 404},
		} {
			rec := httptest.NewRecorder()
			req := httptest.NewRequest(c.method, c.path, strings.NewReader(`{"execution_tag":"gpu","executor_type":"executor_docker_openhands"}`))
			req.Header.Set("Content-Type", "application/json")
			hd.ServeHTTP(rec, req)
			if rec.Code != c.want {
				t.Errorf("%s %s = %d, want %d", c.method, c.path, rec.Code, c.want)
			}
			if c.want == 404 && strings.Contains(rec.Body.String(), "executor_id") {
				t.Errorf("%s %s returned data", c.method, c.path)
			}
		}
		// The HTTP surface read and changed no data.
		if fmt.Sprintf("%+v", h.store.Executors()) != executors || len(h.outbox()) != outbox {
			t.Fatal("HTTP requests changed the store or outbox")
		}
		if len(h.broker.Requests) != requests || len(h.broker.Published) != published || len(h.broker.Consumers) != consumers || len(h.broker.AccountPushes) != pushes {
			t.Fatal("HTTP requests reached the broker")
		}
	})
}
