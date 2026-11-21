package app

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/netip"
	"time"

	"github.com/pandops/flowai-executor-registry/internal/broker"
	"github.com/pandops/flowai-executor-registry/internal/ids"
	"github.com/pandops/flowai-executor-registry/internal/store"
)

type reportedClaim struct {
	TaskID   int64  `json:"task_id"`
	Attempt  int64  `json:"attempt"`
	ClaimSeq int64  `json:"claim_seq"`
	State    string `json:"state"`
}

type heldClaim struct {
	TaskID   int64 `json:"task_id"`
	Attempt  int64 `json:"attempt"`
	ClaimSeq int64 `json:"claim_seq"`
}

type heartbeatBody struct {
	ExecutorType    string          `json:"executor_type"`
	Claims          []reportedClaim `json:"claims"`
	MaxParallel     *int            `json:"max_parallel_tasks"`
	SlotsInUse      *int            `json:"slots_in_use"`
	IntervalSeconds *int            `json:"interval_seconds"`
	MissLimit       *int            `json:"miss_limit"`
	WaitSeconds     *int            `json:"wait_seconds"`
	IP              json.RawMessage `json:"ip"`
}

func inRange(v *int, lo, hi int) bool { return v != nil && *v >= lo && *v <= hi }

func (b heartbeatBody) valid(types func(string) bool) bool {
	if !types(b.ExecutorType) || b.Claims == nil || len(b.Claims) > 1024 {
		return false
	}
	for _, c := range b.Claims {
		if c.TaskID < 1 || c.Attempt < 1 || c.ClaimSeq < 1 || (c.State != "created" && c.State != "pending" && c.State != "running") {
			return false
		}
	}
	return inRange(b.MaxParallel, 0, 1024) && inRange(b.SlotsInUse, 0, 1024) &&
		inRange(b.IntervalSeconds, 1, 86400) && inRange(b.MissLimit, 1, 1000) && inRange(b.WaitSeconds, 1, 604800)
}

// canonicalIP returns the reported IP when it is an exact canonical IPv4 or
// RFC 5952 IPv6 address without zone and not unspecified; otherwise the
// reason it is ignored (missing or invalid).
func canonicalIP(raw json.RawMessage) (string, string) {
	raw = bytes.TrimSpace(raw)
	if len(raw) == 0 || bytes.Equal(raw, []byte("null")) {
		return "", "missing"
	}
	var s string
	if json.Unmarshal(raw, &s) != nil {
		return "", "invalid"
	}
	a, err := netip.ParseAddr(s)
	if err != nil || a.Zone() != "" || a.IsUnspecified() || a.String() != s {
		return "", "invalid"
	}
	return s, ""
}

func available(c store.Connection) int {
	if n := c.MaxParallel - c.SlotsInUse; n > 0 {
		return n
	}
	return 0
}

// statusTx publishes executor_status when the published view changed.
func statusTx(ctx context.Context, tx store.Tx, c *store.Connection, at time.Time) error {
	avail := available(*c)
	if c.StatusPublished && c.StatusState == c.State && c.StatusMax == c.MaxParallel && c.StatusHeld == c.HeldClaims && c.StatusAvailable == avail {
		return nil
	}
	if err := event(ctx, tx, "executor_status", c.TeamID, 0, at, map[string]any{
		"executor_id": c.ExecutorID, "connection_state": c.State, "max_parallel_tasks": c.MaxParallel,
		"held_claims": c.HeldClaims, "available_capacity": avail,
	}); err != nil {
		return err
	}
	c.StatusPublished, c.StatusState, c.StatusMax, c.StatusHeld, c.StatusAvailable = true, c.State, c.MaxParallel, c.HeldClaims, avail
	return nil
}

func (s *Service) heartbeat(ctx context.Context, token string, req broker.Request) (broker.Reply, bool) {
	rid, _ := headerID(req.Header, "request_id")
	id, ok := ids.Parse(token)
	if !ok {
		return s.errReply(errUnknown, rid), true
	}
	env, err := parseEnvelope(req.Data)
	var b heartbeatBody
	if err == nil && len(env.Params) != 0 {
		err = errInvalid
	}
	if err == nil {
		dec := json.NewDecoder(bytes.NewReader(env.Body))
		dec.DisallowUnknownFields()
		if dec.Decode(&b) != nil || !b.valid(s.supportedType) {
			err = errInvalid
		}
	}
	if err != nil {
		return s.errReply(err, rid), true
	}
	ip, reason := canonicalIP(b.IP)
	if reason != "" {
		s.d.Logger.Warn("heartbeat_ip_invalid", "executor_id", id, "reason", reason)
	}
	now := s.now()

	// 1. Record the heartbeat; the team comes from the Executor, never the header.
	var e store.Executor
	var conn store.Connection
	err = s.d.Store.InTx(ctx, func(tx store.Tx) error {
		var err error
		if e, err = tx.GetExecutorByID(ctx, id); err != nil {
			return err
		}
		if conn, err = tx.GetConnection(ctx, id); err != nil {
			return err
		}
		prev := conn.State
		conn.State = store.StateConnected
		conn.LastHeartbeatAt = &now
		conn.IntervalSeconds, conn.MissLimit, conn.WaitSeconds = *b.IntervalSeconds, *b.MissLimit, *b.WaitSeconds
		conn.MaxParallel, conn.SlotsInUse = *b.MaxParallel, *b.SlotsInUse
		if prev == store.StateConnectionLost || prev == store.StateExecutorLost {
			if err := event(ctx, tx, "connection_restored", e.TeamID, 0, now, map[string]any{"executor_id": id}); err != nil {
				return err
			}
		}
		if ip != "" {
			if e.ReportedIP == nil || *e.ReportedIP != ip {
				var old any
				if e.ReportedIP != nil {
					old = *e.ReportedIP
				}
				if err := event(ctx, tx, "ip_changed", e.TeamID, 0, now, map[string]any{"executor_id": id, "old_ip": old, "new_ip": ip}); err != nil {
					return err
				}
			}
			if err := tx.UpdateReportedIP(ctx, id, ip, now); err != nil {
				return err
			}
		}
		return tx.PutConnection(ctx, conn)
	})
	if errors.Is(err, store.ErrNotFound) {
		return s.errReply(errUnknown, rid), true
	}
	if err != nil {
		s.d.Logger.Error("heartbeat_commit_failed", "executor_id", id, "error", err.Error())
		return broker.Reply{}, false
	}

	// 2. Ask task-manager which reported claims it still holds.
	held, known := s.heldClaims(ctx, e, b.Claims, conn.LostSeq)

	// 3. Publish the status when it changed.
	err = s.d.Store.InTx(ctx, func(tx store.Tx) error {
		c, err := tx.GetConnection(ctx, id)
		if err != nil {
			return err
		}
		if known {
			c.HeldClaims = len(held)
		}
		if err := statusTx(ctx, tx, &c, now); err != nil {
			return err
		}
		return tx.PutConnection(ctx, c)
	})
	if err != nil && !errors.Is(err, store.ErrNotFound) {
		s.d.Logger.Error("heartbeat_status_failed", "executor_id", id, "error", err.Error())
	}
	body := map[string]any{"connection_state": store.StateConnected, "claims_known": known}
	if known {
		body["claims"] = held
	}
	return jsonReply(200, body, nil), true
}

// heldClaims requests task-manager.service.executor-claims; known=false
// when task-manager does not answer with a valid reply.
func (s *Service) heldClaims(ctx context.Context, e store.Executor, claims []reportedClaim, lostSeq int64) ([]heldClaim, bool) {
	data, _ := json.Marshal(map[string]any{
		"params": map[string]any{},
		"body":   map[string]any{"team_id": e.TeamID, "executor_id": e.ExecutorID, "claims": claims, "lost_seq": lostSeq},
	})
	cctx, cancel := s.timeout(ctx)
	defer cancel()
	rep, err := s.d.Requester.Request(cctx, subjExecutorClaims,
		broker.Header{"team": {i64(e.TeamID)}, "request_id": {i64(s.d.RequestID())}}, data)
	if err != nil || rep.Status != 200 {
		s.d.Logger.Warn("executor_claims_unknown", "executor_id", e.ExecutorID, "status", rep.Status)
		return nil, false
	}
	var r struct {
		ExecutorID int64       `json:"executor_id"`
		Claims     []heldClaim `json:"claims"`
	}
	if json.Unmarshal(rep.Body, &r) != nil || r.Claims == nil || r.ExecutorID != e.ExecutorID {
		return nil, false
	}
	return r.Claims, true
}

// Sweep evaluates heartbeat deadlines and records connection_lost and
// executor_lost transitions.
func (s *Service) Sweep(ctx context.Context) error {
	now := s.now()
	return s.d.Store.InTx(ctx, func(tx store.Tx) error {
		conns, err := tx.TrackedConnections(ctx)
		if err != nil {
			return err
		}
		for _, c := range conns {
			if c.LastHeartbeatAt == nil {
				continue
			}
			last := *c.LastHeartbeatAt
			elapsed := now.Sub(last)
			changed := false
			if c.State == store.StateConnected && elapsed >= time.Duration(c.IntervalSeconds*c.MissLimit)*time.Second {
				if err := event(ctx, tx, "connection_lost", c.TeamID, 0, now, map[string]any{"executor_id": c.ExecutorID, "last_heartbeat_at": formatTime(last)}); err != nil {
					return err
				}
				c.State, changed = store.StateConnectionLost, true
			}
			if c.State == store.StateConnectionLost && elapsed >= time.Duration(c.WaitSeconds)*time.Second {
				seq, err := lostEvent(ctx, tx, c, now, last)
				if err != nil {
					return err
				}
				c.State, c.LostSeq, changed = store.StateExecutorLost, seq, true
			}
			if !changed {
				continue
			}
			if err := statusTx(ctx, tx, &c, now); err != nil {
				return err
			}
			if err := tx.PutConnection(ctx, c); err != nil {
				return err
			}
		}
		return nil
	})
}

// lostEvent publishes executor_lost and returns its publisher_seq.
func lostEvent(ctx context.Context, tx store.Tx, c store.Connection, now, last time.Time) (int64, error) {
	var seq int64
	err := event(ctx, seqTx{Tx: tx, seq: &seq}, "executor_lost", c.TeamID, 0, now, map[string]any{"executor_id": c.ExecutorID, "last_heartbeat_at": formatTime(last)})
	return seq, err
}

// seqTx records the publisher_seq the wrapped transaction allocates.
type seqTx struct {
	store.Tx
	seq *int64
}

func (t seqTx) NextPublisherSeq(ctx context.Context) (int64, error) {
	n, err := t.Tx.NextPublisherSeq(ctx)
	*t.seq = n
	return n, err
}
