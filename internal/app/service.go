package app

import (
	"context"
	"errors"

	"github.com/pandops/flowai-executor-registry/internal/broker"
	"github.com/pandops/flowai-executor-registry/internal/store"
)

// serviceContext reads the team and request headers of a service request.
func (s *Service) serviceContext(req broker.Request) (team, rid int64, env envelope, err error) {
	rid, _ = headerID(req.Header, "request_id")
	team, ok := headerID(req.Header, "team")
	if !ok {
		return 0, rid, envelope{}, errInvalid
	}
	env, err = parseEnvelope(req.Data)
	if err == nil && env.hasBody() {
		err = errInvalid
	}
	return team, rid, env, err
}

// executorType answers model-registry: the type of one team Executor.
func (s *Service) executorType(ctx context.Context, req broker.Request) broker.Reply {
	team, rid, env, err := s.serviceContext(req)
	if err == nil {
		err = env.allowOnly("executor_id")
	}
	var id int64
	if err == nil {
		id, err = env.id("executor_id")
	}
	if err != nil {
		return s.errReply(err, rid)
	}
	var e store.Executor
	err = s.d.Store.InTx(ctx, func(tx store.Tx) error {
		var err error
		e, err = tx.GetExecutor(ctx, team, id)
		return err
	})
	if errors.Is(err, store.ErrNotFound) {
		return s.errReply(errUnknown, rid)
	}
	if err != nil {
		return s.errReply(errUnavailable, rid)
	}
	return jsonReply(200, map[string]any{
		"executor_id": e.ExecutorID, "team_id": e.TeamID, "execution_tag": e.Tag, "executor_type": e.Type,
	}, nil)
}

// teamTags answers flow-registry: every execution tag of the team.
func (s *Service) teamTags(ctx context.Context, req broker.Request) broker.Reply {
	team, rid, env, err := s.serviceContext(req)
	if err == nil {
		err = env.allowOnly("team_id")
	}
	if err == nil {
		var p int64
		if p, err = env.id("team_id"); err == nil && p != team {
			err = errInvalid
		}
	}
	if err != nil {
		return s.errReply(err, rid)
	}
	var tags []string
	err = s.d.Store.InTx(ctx, func(tx store.Tx) error {
		var err error
		tags, err = tx.ListTags(ctx, team, "", 0)
		return err
	})
	if err != nil {
		return s.errReply(errUnavailable, rid)
	}
	if tags == nil {
		tags = []string{}
	}
	return jsonReply(200, map[string]any{"tags": tags}, nil)
}
