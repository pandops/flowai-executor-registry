// Package outbox relays committed outbox rows to JetStream in
// publisher_seq order until JetStream acknowledges each.
package outbox

import (
	"context"

	"github.com/pandops/flowai-executor-registry/internal/broker"
	"github.com/pandops/flowai-executor-registry/internal/store"
)

const batch = 100

// Relay publishes pending outbox rows.
type Relay struct {
	Store     store.Store
	Publisher broker.Publisher
}

// RunOnce publishes every pending row and returns how many were published.
// It stops at the first row JetStream does not acknowledge, so order holds.
func (r *Relay) RunOnce(ctx context.Context) (int, error) {
	n := 0
	for {
		rows, err := r.Store.PendingOutbox(ctx, batch)
		if err != nil {
			return n, err
		}
		if len(rows) == 0 {
			return n, nil
		}
		for _, row := range rows {
			h := broker.Header{}
			for k, v := range row.Header {
				h[k] = []string{v}
			}
			if err := r.Publisher.PublishStream(ctx, broker.Message{Subject: row.Subject, Header: h, Data: row.Data}); err != nil {
				return n, err
			}
			if err := r.Store.MarkPublished(ctx, row.Seq); err != nil {
				return n, err
			}
			n++
		}
	}
}
