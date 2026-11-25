// Package mocks holds in-process test fakes of executor-registry's
// boundaries: an in-memory store with PostgreSQL-like sequences and a fake
// broker. Production code never imports this package.
package mocks

import (
	"context"
	"errors"
	"sort"
	"strconv"
	"sync"
	"time"

	"github.com/pandops/flowai-executor-registry/internal/store"
)

type memState struct {
	executors map[int64]store.Executor
	conns     map[int64]store.Connection
	consumers map[string]string
	outbox    []store.OutboxRow
}

func (s memState) clone() memState {
	c := memState{
		executors: make(map[int64]store.Executor, len(s.executors)),
		conns:     make(map[int64]store.Connection, len(s.conns)),
		consumers: make(map[string]string, len(s.consumers)),
		outbox:    append([]store.OutboxRow(nil), s.outbox...),
	}
	for k, v := range s.executors {
		c.executors[k] = v
	}
	for k, v := range s.conns {
		c.conns[k] = v
	}
	for k, v := range s.consumers {
		c.consumers[k] = v
	}
	return c
}

// MemStore is an in-memory store.Store. Transactions are serialized and
// atomic; sequences are not rolled back, like PostgreSQL sequences.
type MemStore struct {
	mu        sync.Mutex
	state     memState
	published map[int64]bool
	execSeq   int64
	pubSeq    int64
	// FailNextCommit makes the next transaction fail at commit after fn ran.
	FailNextCommit error
	// FailPing makes Ping fail.
	FailPing error
}

// NewMemStore returns an empty store.
func NewMemStore() *MemStore {
	return &MemStore{state: memState{
		executors: map[int64]store.Executor{},
		conns:     map[int64]store.Connection{},
		consumers: map[string]string{},
	}, published: map[int64]bool{}}
}

// SetNextPublisherSeq makes the next allocated publisher_seq equal n.
func (m *MemStore) SetNextPublisherSeq(n int64) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.pubSeq = n - 1
}

// Outbox returns every committed outbox row in seq order.
func (m *MemStore) Outbox() []store.OutboxRow {
	m.mu.Lock()
	defer m.mu.Unlock()
	return append([]store.OutboxRow(nil), m.state.outbox...)
}

// Executors returns every stored Executor.
func (m *MemStore) Executors() []store.Executor {
	m.mu.Lock()
	defer m.mu.Unlock()
	var out []store.Executor
	for _, e := range m.state.executors {
		out = append(out, e)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ExecutorID < out[j].ExecutorID })
	return out
}

// Connection returns the stored connection record.
func (m *MemStore) Connection(id int64) (store.Connection, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	c, ok := m.state.conns[id]
	return c, ok
}

// SetConnection overwrites a stored connection record (test setup).
func (m *MemStore) SetConnection(c store.Connection) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.state.conns[c.ExecutorID] = c
}

// TaskConsumers returns the recorded task consumer names.
func (m *MemStore) TaskConsumers() []string {
	m.mu.Lock()
	defer m.mu.Unlock()
	var out []string
	for _, v := range m.state.consumers {
		out = append(out, v)
	}
	sort.Strings(out)
	return out
}

func (m *MemStore) InTx(ctx context.Context, fn func(store.Tx) error) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	tx := &memTx{m: m, s: m.state.clone()}
	if err := fn(tx); err != nil {
		return err
	}
	if err := m.FailNextCommit; err != nil {
		m.FailNextCommit = nil
		return err
	}
	m.state = tx.s
	return nil
}

func (m *MemStore) PendingOutbox(ctx context.Context, limit int) ([]store.OutboxRow, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	var out []store.OutboxRow
	for _, r := range m.state.outbox {
		if !m.published[r.Seq] {
			out = append(out, r)
			if limit > 0 && len(out) == limit {
				break
			}
		}
	}
	return out, nil
}

func (m *MemStore) MarkPublished(ctx context.Context, seq int64) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.published[seq] = true
	return nil
}

func (m *MemStore) Ping(ctx context.Context) error { return m.FailPing }

type memTx struct {
	m *MemStore
	s memState
}

func consumerKey(team int64, tag string) string {
	return strconv.FormatInt(team, 10) + "\x00" + tag
}

func (t *memTx) NextExecutorID(ctx context.Context) (int64, error) {
	t.m.execSeq++
	return t.m.execSeq, nil
}

func (t *memTx) NextPublisherSeq(ctx context.Context) (int64, error) {
	t.m.pubSeq++
	return t.m.pubSeq, nil
}

func (t *memTx) InsertExecutor(ctx context.Context, e store.Executor) error {
	if _, ok := t.s.executors[e.ExecutorID]; ok {
		return errors.New("memstore: duplicate executor_id")
	}
	t.s.executors[e.ExecutorID] = e
	t.s.conns[e.ExecutorID] = store.Connection{
		ExecutorID: e.ExecutorID, TeamID: e.TeamID, State: store.StateNeverConnected,
		IntervalSeconds: store.DefaultIntervalSeconds, MissLimit: store.DefaultMissLimit, WaitSeconds: store.DefaultWaitSeconds,
	}
	return nil
}

func (t *memTx) GetExecutor(ctx context.Context, teamID, executorID int64) (store.Executor, error) {
	e, ok := t.s.executors[executorID]
	if !ok || e.TeamID != teamID {
		return store.Executor{}, store.ErrNotFound
	}
	return e, nil
}

func (t *memTx) GetExecutorByID(ctx context.Context, executorID int64) (store.Executor, error) {
	e, ok := t.s.executors[executorID]
	if !ok {
		return store.Executor{}, store.ErrNotFound
	}
	return e, nil
}

func (t *memTx) UpdateToken(ctx context.Context, id int64, pub, fp string, at time.Time) error {
	e, ok := t.s.executors[id]
	if !ok {
		return store.ErrNotFound
	}
	e.UserPublicKey, e.TokenFingerprint, e.TokenIssuedAt = pub, fp, at
	t.s.executors[id] = e
	return nil
}

func (t *memTx) UpdateReportedIP(ctx context.Context, id int64, ip string, at time.Time) error {
	e, ok := t.s.executors[id]
	if !ok {
		return store.ErrNotFound
	}
	e.ReportedIP, e.IPReportedAt = &ip, &at
	t.s.executors[id] = e
	return nil
}

func (t *memTx) DeleteExecutor(ctx context.Context, id int64) error {
	if _, ok := t.s.executors[id]; !ok {
		return store.ErrNotFound
	}
	delete(t.s.executors, id)
	delete(t.s.conns, id)
	return nil
}

func (t *memTx) ListExecutors(ctx context.Context, teamID, after int64, limit int) ([]store.Executor, error) {
	var out []store.Executor
	for _, e := range t.s.executors {
		if e.TeamID == teamID && e.ExecutorID > after {
			out = append(out, e)
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ExecutorID < out[j].ExecutorID })
	if limit > 0 && len(out) > limit {
		out = out[:limit]
	}
	return out, nil
}

func (t *memTx) CountExecutors(ctx context.Context, teamID int64) (int, error) {
	n := 0
	for _, e := range t.s.executors {
		if e.TeamID == teamID {
			n++
		}
	}
	return n, nil
}

func (t *memTx) ListTags(ctx context.Context, teamID int64, after string, limit int) ([]string, error) {
	set := map[string]bool{}
	for _, e := range t.s.executors {
		if e.TeamID == teamID && e.Tag > after {
			set[e.Tag] = true
		}
	}
	var out []string
	for k := range set {
		out = append(out, k)
	}
	sort.Strings(out)
	if limit > 0 && len(out) > limit {
		out = out[:limit]
	}
	return out, nil
}

func (t *memTx) TaskConsumerRecorded(ctx context.Context, teamID int64, tag string) (bool, error) {
	_, ok := t.s.consumers[consumerKey(teamID, tag)]
	return ok, nil
}

func (t *memTx) RecordTaskConsumer(ctx context.Context, teamID int64, tag, name string) error {
	t.s.consumers[consumerKey(teamID, tag)] = name
	return nil
}

func (t *memTx) GetConnection(ctx context.Context, id int64) (store.Connection, error) {
	c, ok := t.s.conns[id]
	if !ok {
		return store.Connection{}, store.ErrNotFound
	}
	return c, nil
}

func (t *memTx) PutConnection(ctx context.Context, c store.Connection) error {
	if _, ok := t.s.conns[c.ExecutorID]; !ok {
		return store.ErrNotFound
	}
	t.s.conns[c.ExecutorID] = c
	return nil
}

func (t *memTx) TrackedConnections(ctx context.Context) ([]store.Connection, error) {
	var out []store.Connection
	for _, c := range t.s.conns {
		if c.State == store.StateConnected || c.State == store.StateConnectionLost {
			out = append(out, c)
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ExecutorID < out[j].ExecutorID })
	return out, nil
}

func (t *memTx) InsertOutbox(ctx context.Context, row store.OutboxRow) error {
	t.s.outbox = append(t.s.outbox, row)
	return nil
}
