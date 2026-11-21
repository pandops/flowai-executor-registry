//go:build integration

package test

import (
	"context"
	"errors"
	"testing"

	"github.com/pandops/flowai-executor-registry/internal/store"
)

// Coverage: "Outbox publisher_seq comes from the service's own sequence in
// the committing transaction: a rolled-back transaction publishes nothing,
// and a restart continues the sequence, so a new message never reuses a
// Nats-Msg-Id" (1.7).

func writeOutbox(ctx context.Context, s store.Store, fail error) (int64, error) {
	var seq int64
	err := s.InTx(ctx, func(tx store.Tx) error {
		var err error
		if seq, err = tx.NextPublisherSeq(ctx); err != nil {
			return err
		}
		if err := tx.InsertOutbox(ctx, store.OutboxRow{Seq: seq, Subject: "executor-registry.event.executor_status.pub",
			Header: map[string]string{"team": "3", "Nats-Msg-Id": "executor-registry-x"}, Data: []byte(`{"publisher_seq":1}`)}); err != nil {
			return err
		}
		return fail
	})
	return seq, err
}

func TestPublisherSeq(t *testing.T) {
	ctx := context.Background()
	d := newTestDB(t, true)
	s := d.openStore(t)

	a, err := writeOutbox(ctx, s, nil)
	if err != nil || a < 1 {
		t.Fatalf("seq %d %v", a, err)
	}
	rolled, err := writeOutbox(ctx, s, errors.New("rollback"))
	if err == nil {
		t.Fatal("rollback not reported")
	}
	pending, err := s.PendingOutbox(ctx, 100)
	if err != nil || len(pending) != 1 || pending[0].Seq != a {
		t.Fatalf("pending %+v %v", pending, err)
	}
	if pending[0].Header["team"] != "3" || string(pending[0].Data) != `{"publisher_seq":1}` {
		t.Fatalf("row %+v", pending[0])
	}
	if err := s.MarkPublished(ctx, a); err != nil {
		t.Fatal(err)
	}
	if p, _ := s.PendingOutbox(ctx, 100); len(p) != 0 {
		t.Fatalf("published row still pending: %+v", p)
	}
	s.Close()

	// Restart: a new store on the same database continues the sequence.
	s2 := d.openStore(t)
	b, err := writeOutbox(ctx, s2, nil)
	if err != nil || b <= rolled || b <= a {
		t.Fatalf("seq after restart %d (a %d, rolled %d) %v", b, a, rolled, err)
	}
	p, _ := s2.PendingOutbox(ctx, 100)
	if len(p) != 1 || p[0].Seq != b {
		t.Fatalf("pending after restart %+v", p)
	}
}
