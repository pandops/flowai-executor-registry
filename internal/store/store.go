// Package store defines executor-registry's persistence contract. The
// PostgreSQL implementation is internal/store/pgstore; unit tests use the
// in-memory fake in internal/mocks.
package store

import (
	"context"
	"errors"
	"time"
)

// Connection view states (design rule 7).
const (
	StateNeverConnected = "never_connected"
	StateConnected      = "connected"
	StateConnectionLost = "connection_lost"
	StateExecutorLost   = "executor_lost"
)

// Default heartbeat timing before the first successful heartbeat (rule 22).
const (
	DefaultIntervalSeconds = 15
	DefaultMissLimit       = 3
	DefaultWaitSeconds     = 600
)

// ErrNotFound is returned when a row does not exist.
var ErrNotFound = errors.New("store: not found")

// Executor is one team Executor. The JWT itself is never stored.
type Executor struct {
	ExecutorID       int64
	TeamID           int64
	Tag              string
	Type             string
	CreatedAt        time.Time
	UserPublicKey    string
	TokenFingerprint string
	TokenIssuedAt    time.Time
	ReportedIP       *string
	IPReportedAt     *time.Time
}

// Connection is executor-registry's connection record of one Executor.
type Connection struct {
	ExecutorID      int64
	TeamID          int64
	State           string
	LastHeartbeatAt *time.Time
	IntervalSeconds int
	MissLimit       int
	WaitSeconds     int
	MaxParallel     int
	SlotsInUse      int
	HeldClaims      int
	// The last published executor_status values; StatusPublished is false
	// until the first executor_status.
	StatusPublished bool
	StatusState     string
	StatusMax       int
	StatusHeld      int
	StatusAvailable int
	// LostSeq is the publisher_seq of the latest executor_lost (0 if none).
	LostSeq int64
}

// OutboxRow is one stream message written in a business transaction.
type OutboxRow struct {
	Seq     int64
	Subject string
	Header  map[string]string
	Data    []byte
}

// Tx is one database transaction.
type Tx interface {
	NextExecutorID(ctx context.Context) (int64, error)
	NextPublisherSeq(ctx context.Context) (int64, error)
	InsertExecutor(ctx context.Context, e Executor) error
	// GetExecutor finds an Executor only when it belongs to teamID.
	GetExecutor(ctx context.Context, teamID, executorID int64) (Executor, error)
	// GetExecutorByID resolves an Executor subject (team derived from it).
	GetExecutorByID(ctx context.Context, executorID int64) (Executor, error)
	UpdateToken(ctx context.Context, executorID int64, userPublicKey, fingerprint string, issuedAt time.Time) error
	UpdateReportedIP(ctx context.Context, executorID int64, ip string, at time.Time) error
	DeleteExecutor(ctx context.Context, executorID int64) error
	// ListExecutors returns the team's Executors with executor_id > after, ascending.
	ListExecutors(ctx context.Context, teamID, after int64, limit int) ([]Executor, error)
	CountExecutors(ctx context.Context, teamID int64) (int, error)
	// ListTags returns the team's distinct tags greater than after, lexical order; limit <= 0 means all.
	ListTags(ctx context.Context, teamID int64, after string, limit int) ([]string, error)
	TaskConsumerRecorded(ctx context.Context, teamID int64, tag string) (bool, error)
	RecordTaskConsumer(ctx context.Context, teamID int64, tag, name string) error
	GetConnection(ctx context.Context, executorID int64) (Connection, error)
	PutConnection(ctx context.Context, c Connection) error
	// TrackedConnections returns connections in state connected or connection_lost.
	TrackedConnections(ctx context.Context) ([]Connection, error)
	InsertOutbox(ctx context.Context, row OutboxRow) error
}

// Store is the database.
type Store interface {
	InTx(ctx context.Context, fn func(Tx) error) error
	PendingOutbox(ctx context.Context, limit int) ([]OutboxRow, error)
	MarkPublished(ctx context.Context, seq int64) error
	Ping(ctx context.Context) error
}
