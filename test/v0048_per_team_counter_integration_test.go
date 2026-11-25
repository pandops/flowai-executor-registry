//go:build integration

package test

import (
	"context"
	"errors"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/pandops/flowai-executor-registry/internal/store"
)

// Coverage: "Per-team counters under concurrent creates ... give unique,
// increasing numbers; gaps after a rollback are allowed and no number is
// reused; the first record in a new team is 1" (1.7). executor_id is one
// global sequence (executor-registry spec), so the first Executor of a new
// deployment is 1 and a new team continues the global sequence.

func insertExecutor(ctx context.Context, s store.Store, team int64, fail error) (int64, error) {
	var id int64
	err := s.InTx(ctx, func(tx store.Tx) error {
		var err error
		if id, err = tx.NextExecutorID(ctx); err != nil {
			return err
		}
		now := time.Now().UTC()
		if err := tx.InsertExecutor(ctx, store.Executor{ExecutorID: id, TeamID: team, Tag: "gpu", Type: "executor_docker_openhands", CreatedAt: now, UserPublicKey: "U", TokenFingerprint: strings.Repeat("f", 64), TokenIssuedAt: now}); err != nil {
			return err
		}
		return fail
	})
	return id, err
}

func TestExecutorIDCounter(t *testing.T) {
	ctx := context.Background()
	d := newTestDB(t, true)
	s := d.openStore(t)

	first, err := insertExecutor(ctx, s, 3, nil)
	if err != nil || first != 1 {
		t.Fatalf("first id %d %v", first, err)
	}
	var wg sync.WaitGroup
	var mu sync.Mutex
	var got []int64
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			id, err := insertExecutor(ctx, s, 3+int64(i%2), nil)
			if err != nil {
				t.Errorf("create: %v", err)
				return
			}
			mu.Lock()
			got = append(got, id)
			mu.Unlock()
		}(i)
	}
	wg.Wait()
	sort.Slice(got, func(i, j int) bool { return got[i] < got[j] })
	for i := 1; i < len(got); i++ {
		if got[i] == got[i-1] {
			t.Fatalf("duplicate id %d", got[i])
		}
	}
	if len(got) != 20 || got[0] <= first {
		t.Fatalf("ids %v", got)
	}
	rolled, err := insertExecutor(ctx, s, 3, errors.New("rollback"))
	if err == nil {
		t.Fatal("rollback not reported")
	}
	if err := s.InTx(ctx, func(tx store.Tx) error {
		if _, err := tx.GetExecutorByID(ctx, rolled); !errors.Is(err, store.ErrNotFound) {
			t.Errorf("rolled-back Executor %d exists: %v", rolled, err)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	next, err := insertExecutor(ctx, s, 9, nil) // a new team continues the global sequence
	if err != nil || next <= rolled || next <= got[len(got)-1] {
		t.Fatalf("id after rollback %d (rolled %d, max %d) %v", next, rolled, got[len(got)-1], err)
	}
	// A deleted Executor's number is never reused.
	s.InTx(ctx, func(tx store.Tx) error { return tx.DeleteExecutor(ctx, next) })
	again, _ := insertExecutor(ctx, s, 9, nil)
	if again <= next {
		t.Fatalf("id %d reused after delete of %d", again, next)
	}
}
