// Package server builds what the binary serves and connects with: the HTTP
// handler (only the probes, ready while the database answers and every
// broker connection is up) and the NATS connection options that drive it.
package server

import (
	"context"
	"net/http"
	"time"

	"github.com/nats-io/nats.go"

	"github.com/pandops/flowai-executor-registry/internal/config"
	"github.com/pandops/flowai-executor-registry/internal/natsbroker"
	"github.com/pandops/flowai-executor-registry/internal/probes"
	"github.com/pandops/flowai-executor-registry/internal/store"
)

// Handler returns the probe handler over the store and connection states.
func Handler(st store.Store, connected ...func() bool) http.Handler {
	ping := func() error {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		return st.Ping(ctx)
	}
	all := func() bool {
		for _, c := range connected {
			if !c() {
				return false
			}
		}
		return len(connected) > 0
	}
	return probes.Handler(probes.Ready(ping, all))
}

// Wiring is what the binary serves and connects with: the HTTP handler and
// the options of the service and system-account NATS connections, whose
// connection handlers drive the handler's readiness.
type Wiring struct {
	Handler http.Handler
	Service []nats.Option
	System  []nats.Option
}

// Wire builds the Wiring from the configuration and the store.
func Wire(cfg config.Config, st store.Store) (Wiring, error) {
	svcStatus, sysStatus := natsbroker.NewStatus(), natsbroker.NewStatus()
	svc, err := natsbroker.Options(cfg.NATS, svcStatus)
	if err != nil {
		return Wiring{}, err
	}
	sys, err := natsbroker.SystemOptions(cfg.NATS, cfg.NATSKeys.SystemCredentialsFile, sysStatus)
	if err != nil {
		return Wiring{}, err
	}
	return Wiring{Handler: Handler(st, svcStatus.Connected, sysStatus.Connected), Service: svc, System: sys}, nil
}
