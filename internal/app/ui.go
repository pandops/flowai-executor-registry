package app

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"regexp"
	"strconv"
	"time"

	"github.com/pandops/flowai-executor-registry/internal/broker"
	"github.com/pandops/flowai-executor-registry/internal/cursor"
	"github.com/pandops/flowai-executor-registry/internal/natsjwt"
	"github.com/pandops/flowai-executor-registry/internal/store"
)

var tagPattern = regexp.MustCompile(`^[A-Za-z0-9_-]{1,128}$`)

// operator is the verified context API Gateway forwards (rule 9).
type operator struct {
	team, operator, request int64
}

// audit is one human action record (rule 10).
type audit struct {
	action     string
	resourceID int64 // 0 = null
	outcome    string
	reason     string
}

func (s *Service) handleUI(ctx context.Context, op string, req broker.Request) broker.Reply {
	team, ok1 := headerID(req.Header, "team")
	opID, ok2 := headerID(req.Header, "operator_id")
	rid, ok3 := headerID(req.Header, "request_id")
	if !ok1 || !ok2 || !ok3 {
		return s.errReply(errInvalid, rid)
	}
	o := operator{team: team, operator: opID, request: rid}
	env, err := parseEnvelope(req.Data)
	if err != nil {
		return s.rejectAudited(ctx, op, o, 0, err)
	}
	switch op {
	case "create":
		return s.create(ctx, o, env)
	case "list":
		return s.list(ctx, o, env)
	case "get":
		return s.get(ctx, o, env)
	case "reissue":
		return s.reissue(ctx, o, env)
	case "delete":
		return s.remove(ctx, o, env)
	case "list-execution-tags":
		return s.listTags(ctx, o, env)
	}
	return s.errReply(errInvalid, rid)
}

var auditActions = map[string]string{"create": "executor_created", "reissue": "executor_token_reissued", "delete": "executor_deleted"}

// rejectAudited answers err and, for an audited action, records the
// rejection in its own transaction.
func (s *Service) rejectAudited(ctx context.Context, op string, o operator, resourceID int64, err error) broker.Reply {
	a := asAPI(err)
	if action, ok := auditActions[op]; ok {
		s.writeAudit(ctx, o, audit{action: action, resourceID: resourceID, outcome: "rejected", reason: a.code})
	}
	return s.errReply(a, o.request)
}

func (s *Service) writeAudit(ctx context.Context, o operator, a audit) {
	err := s.d.Store.InTx(ctx, func(tx store.Tx) error { return auditTx(ctx, tx, o, a, s.now()) })
	if err != nil {
		s.d.Logger.Error("audit_write_failed", "action", a.action, "request_id", o.request, "error", err.Error())
	}
}

func auditTx(ctx context.Context, tx store.Tx, o operator, a audit, at time.Time) error {
	var resource any
	if a.resourceID > 0 {
		resource = a.resourceID
	}
	fields := map[string]any{
		"source_service": ServiceName,
		"actor_type":     "operator",
		"actor_id":       o.operator,
		"action":         a.action,
		"resource_type":  "executor",
		"resource_id":    resource,
		"request_id":     o.request,
		"outcome":        a.outcome,
	}
	if a.reason != "" {
		fields["reason"] = a.reason
	}
	return publish(ctx, tx, ServiceName+".audit."+a.action+".pub", o.team, o.request, at, fields)
}

// teamParam checks the path team equals the trusted team header.
func (o operator) teamParam(env envelope) error {
	team, err := env.id("team_id")
	if err != nil || team != o.team {
		return errInvalid
	}
	return nil
}

func view(e store.Executor) map[string]any {
	var ip, ipAt any
	if e.ReportedIP != nil {
		ip = *e.ReportedIP
	}
	if e.IPReportedAt != nil {
		ipAt = formatTime(*e.IPReportedAt)
	}
	return map[string]any{
		"executor_id":       e.ExecutorID,
		"team_id":           e.TeamID,
		"execution_tag":     e.Tag,
		"executor_type":     e.Type,
		"created_at":        formatTime(e.CreatedAt),
		"token_fingerprint": e.TokenFingerprint,
		"token_issued_at":   formatTime(e.TokenIssuedAt),
		"reported_ip":       ip,
		"ip_reported_at":    ipAt,
	}
}

// ---- create ----

type createBody struct {
	tag, typ string
}

func parseCreateBody(env envelope) (createBody, error) {
	var m map[string]json.RawMessage
	if !env.hasBody() || json.Unmarshal(env.Body, &m) != nil || m == nil {
		return createBody{}, errInvalid
	}
	for k := range m {
		if k != "execution_tag" && k != "executor_type" {
			return createBody{}, errInvalid
		}
	}
	raw, ok := m["execution_tag"]
	raw = bytes.TrimSpace(raw)
	if !ok || bytes.Equal(raw, []byte("null")) || (len(raw) > 0 && raw[0] == '[') {
		return createBody{}, errTagCount
	}
	var b createBody
	if json.Unmarshal(raw, &b.tag) != nil || !tagPattern.MatchString(b.tag) {
		return createBody{}, errInvalid
	}
	if json.Unmarshal(m["executor_type"], &b.typ) != nil {
		return createBody{}, errInvalid
	}
	return b, nil
}

func (s *Service) create(ctx context.Context, o operator, env envelope) broker.Reply {
	if err := env.allowOnly("team_id"); err != nil {
		return s.rejectAudited(ctx, "create", o, 0, err)
	}
	if err := o.teamParam(env); err != nil {
		return s.rejectAudited(ctx, "create", o, 0, err)
	}
	b, err := parseCreateBody(env)
	if err == nil && !s.supportedType(b.typ) {
		err = errInvalid
	}
	if err != nil {
		return s.rejectAudited(ctx, "create", o, 0, err)
	}
	if err := s.checkTeam(ctx, o); err != nil {
		return s.rejectAudited(ctx, "create", o, 0, err)
	}
	now := s.now()
	var created store.Executor
	var token string
	err = s.d.Store.InTx(ctx, func(tx store.Tx) error {
		id, err := tx.NextExecutorID(ctx)
		if err != nil {
			return err
		}
		recorded, err := tx.TaskConsumerRecorded(ctx, o.team, b.tag)
		if err != nil {
			return err
		}
		if !recorded {
			name := natsjwt.TaskConsumerName(o.team, b.tag)
			cctx, cancel := s.timeout(ctx)
			err := s.d.Consumers.CreateTaskConsumer(cctx, broker.TaskConsumer{
				Stream: taskStream, Durable: name,
				FilterSubject: "task-manager.task." + i64(o.team) + "." + b.tag + ".*.sub",
			})
			cancel()
			if err != nil {
				s.d.Logger.Error("task_consumer_create_failed", "team_id", o.team, "consumer", name, "error", err.Error())
				return brokerError(err)
			}
			if err := tx.RecordTaskConsumer(ctx, o.team, b.tag, name); err != nil {
				return err
			}
		}
		issued, err := s.d.Signer.IssueExecutor(natsjwt.Identity{TeamID: o.team, ExecutorID: id, Tag: b.tag, ExecutorType: b.typ}, now)
		if err != nil {
			return err
		}
		created = store.Executor{
			ExecutorID: id, TeamID: o.team, Tag: b.tag, Type: b.typ, CreatedAt: now,
			UserPublicKey: issued.UserPublicKey, TokenFingerprint: issued.Fingerprint, TokenIssuedAt: issued.IssuedAt,
		}
		if err := tx.InsertExecutor(ctx, created); err != nil {
			return err
		}
		if err := auditTx(ctx, tx, o, audit{action: "executor_created", resourceID: id, outcome: "succeeded"}, now); err != nil {
			return err
		}
		if err := event(ctx, tx, "executor_registered", o.team, o.request, now, map[string]any{
			"executor_id": id, "execution_tag": b.tag, "executor_type": b.typ,
		}); err != nil {
			return err
		}
		token = issued.JWT
		return nil
	})
	if err != nil {
		s.logFailure("create", o, err)
		return s.rejectAudited(ctx, "create", o, 0, err)
	}
	body := view(created)
	body["token"] = token
	return jsonReply(201, body, broker.Header{"Location": {"/ui/v1/teams/" + i64(o.team) + "/executors/" + i64(created.ExecutorID)}})
}

func (s *Service) logFailure(op string, o operator, err error) {
	var a *apiError
	if errors.As(err, &a) {
		return
	}
	s.d.Logger.Error("operation_failed", "operation", op, "team_id", o.team, "request_id", o.request, "error", err.Error())
}

// checkTeam asks team-registry whether the team exists and is active.
func (s *Service) checkTeam(ctx context.Context, o operator) error {
	cctx, cancel := s.timeout(ctx)
	defer cancel()
	rep, err := s.d.Requester.Request(cctx, subjTeamGet,
		broker.Header{"team": {i64(o.team)}, "request_id": {i64(o.request)}},
		[]byte(`{"params":{"team_id":`+i64(o.team)+`}}`))
	if err != nil {
		return brokerError(err)
	}
	switch rep.Status {
	case 200:
		var t struct {
			TeamID     int64           `json:"team_id"`
			ArchivedAt json.RawMessage `json:"archived_at"`
		}
		if json.Unmarshal(rep.Body, &t) != nil || t.TeamID != o.team {
			return errUnavailable
		}
		if a := bytes.TrimSpace(t.ArchivedAt); len(a) > 0 && !bytes.Equal(a, []byte("null")) {
			return errArchived
		}
		return nil
	case 404:
		return errTeamUnknown
	}
	return errUnavailable
}

// ---- reads ----

func (s *Service) get(ctx context.Context, o operator, env envelope) broker.Reply {
	if err := env.allowOnly("team_id", "executor_id"); err != nil {
		return s.errReply(err, o.request)
	}
	if err := o.teamParam(env); err != nil {
		return s.errReply(err, o.request)
	}
	id, err := env.id("executor_id")
	if err != nil {
		return s.errReply(err, o.request)
	}
	var e store.Executor
	err = s.d.Store.InTx(ctx, func(tx store.Tx) error {
		var err error
		e, err = tx.GetExecutor(ctx, o.team, id)
		return err
	})
	if errors.Is(err, store.ErrNotFound) {
		return s.errReply(errUnknown, o.request)
	}
	if err != nil {
		s.logFailure("get", o, err)
		return s.errReply(errUnavailable, o.request)
	}
	return jsonReply(200, view(e), nil)
}

func (s *Service) list(ctx context.Context, o operator, env envelope) broker.Reply {
	if err := env.allowOnly("team_id", "cursor", "limit"); err != nil {
		return s.errReply(err, o.request)
	}
	if err := o.teamParam(env); err != nil {
		return s.errReply(err, o.request)
	}
	cur, limit, err := env.page()
	if err != nil {
		return s.errReply(err, o.request)
	}
	var after int64
	if cur != "" {
		p, err := cursor.Decode(s.d.Cursors, cur, "list", o.team)
		if err != nil {
			return s.errReply(errPagination, o.request)
		}
		if after, err = strconv.ParseInt(p.After, 10, 64); err != nil || after < 1 {
			return s.errReply(errPagination, o.request)
		}
	}
	var rows []store.Executor
	var total int
	err = s.d.Store.InTx(ctx, func(tx store.Tx) error {
		var err error
		if rows, err = tx.ListExecutors(ctx, o.team, after, limit+1); err != nil {
			return err
		}
		total, err = tx.CountExecutors(ctx, o.team)
		return err
	})
	if err != nil {
		s.logFailure("list", o, err)
		return s.errReply(errUnavailable, o.request)
	}
	var next any
	if len(rows) > limit {
		rows = rows[:limit]
		tok, err := cursor.Encode(s.d.Cursors, cursor.Position{Operation: "list", TeamID: o.team, After: i64(rows[len(rows)-1].ExecutorID)})
		if err != nil {
			return s.errReply(errUnavailable, o.request)
		}
		next = tok
	}
	items := make([]map[string]any, 0, len(rows))
	for _, e := range rows {
		items = append(items, view(e))
	}
	return jsonReply(200, map[string]any{"items": items, "next_cursor": next, "total_count": total}, nil)
}

func (s *Service) listTags(ctx context.Context, o operator, env envelope) broker.Reply {
	if err := env.allowOnly("cursor", "limit"); err != nil {
		return s.errReply(err, o.request)
	}
	cur, limit, err := env.page()
	if err != nil {
		return s.errReply(err, o.request)
	}
	after := ""
	if cur != "" {
		p, err := cursor.Decode(s.d.Cursors, cur, "list-execution-tags", o.team)
		if err != nil || p.After == "" {
			return s.errReply(errPagination, o.request)
		}
		after = p.After
	}
	var tags []string
	err = s.d.Store.InTx(ctx, func(tx store.Tx) error {
		var err error
		tags, err = tx.ListTags(ctx, o.team, after, limit+1)
		return err
	})
	if err != nil {
		s.logFailure("list-execution-tags", o, err)
		return s.errReply(errUnavailable, o.request)
	}
	var next any
	if len(tags) > limit {
		tags = tags[:limit]
		tok, err := cursor.Encode(s.d.Cursors, cursor.Position{Operation: "list-execution-tags", TeamID: o.team, After: tags[len(tags)-1]})
		if err != nil {
			return s.errReply(errUnavailable, o.request)
		}
		next = tok
	}
	if tags == nil {
		tags = []string{}
	}
	return jsonReply(200, map[string]any{"items": tags, "next_cursor": next}, nil)
}

// ---- reissue and delete ----

// target resolves the Executor of a reissue or delete within the trusted team.
func (s *Service) target(ctx context.Context, op string, o operator, env envelope) (store.Executor, *broker.Reply) {
	fail := func(err error) (store.Executor, *broker.Reply) {
		r := s.rejectAudited(ctx, op, o, 0, err)
		return store.Executor{}, &r
	}
	if err := env.allowOnly("team_id", "executor_id"); err != nil {
		return fail(err)
	}
	if err := o.teamParam(env); err != nil {
		return fail(err)
	}
	if env.hasBody() {
		return fail(errInvalid)
	}
	id, err := env.id("executor_id")
	if err != nil {
		return fail(err)
	}
	var e store.Executor
	err = s.d.Store.InTx(ctx, func(tx store.Tx) error {
		var err error
		e, err = tx.GetExecutor(ctx, o.team, id)
		return err
	})
	if errors.Is(err, store.ErrNotFound) {
		return fail(errUnknown)
	}
	if err != nil {
		s.logFailure(op, o, err)
		return fail(errUnavailable)
	}
	return e, nil
}

// revoke adds userPublicKey to the live account JWT's revocations and
// pushes it. It never retries: a failure leaves the old state.
func (s *Service) revoke(ctx context.Context, userPublicKey string) error {
	s.account.Lock()
	defer s.account.Unlock()
	cctx, cancel := s.timeout(ctx)
	defer cancel()
	live, err := s.d.Accounts.LookupAccountJWT(cctx, s.d.Signer.AccountPublicKey())
	if err != nil {
		s.d.Logger.Error("account_lookup_failed", "error", err.Error())
		return brokerError(err)
	}
	updated, err := s.d.Signer.RevokeUser(live, userPublicKey, s.now())
	if err != nil {
		s.d.Logger.Error("account_resign_failed", "error", err.Error())
		return errUnavailable
	}
	if err := s.d.Accounts.UpdateAccountJWT(cctx, updated); err != nil {
		s.d.Logger.Error("account_update_failed", "error", err.Error())
		return brokerError(err)
	}
	return nil
}

func (s *Service) reissue(ctx context.Context, o operator, env envelope) broker.Reply {
	e, rep := s.target(ctx, "reissue", o, env)
	if rep != nil {
		return *rep
	}
	now := s.now()
	issued, err := s.d.Signer.IssueExecutor(natsjwt.Identity{TeamID: e.TeamID, ExecutorID: e.ExecutorID, Tag: e.Tag, ExecutorType: e.Type}, now)
	if err != nil {
		s.logFailure("reissue", o, err)
		return s.rejectAudited(ctx, "reissue", o, e.ExecutorID, errUnavailable)
	}
	if err := s.revoke(ctx, e.UserPublicKey); err != nil {
		return s.rejectAudited(ctx, "reissue", o, e.ExecutorID, err)
	}
	err = s.d.Store.InTx(ctx, func(tx store.Tx) error {
		if err := tx.UpdateToken(ctx, e.ExecutorID, issued.UserPublicKey, issued.Fingerprint, issued.IssuedAt); err != nil {
			return err
		}
		return auditTx(ctx, tx, o, audit{action: "executor_token_reissued", resourceID: e.ExecutorID, outcome: "succeeded"}, now)
	})
	if err != nil {
		s.logFailure("reissue", o, err)
		return s.rejectAudited(ctx, "reissue", o, e.ExecutorID, errUnavailable)
	}
	return jsonReply(200, map[string]any{
		"executor_id": e.ExecutorID, "token": issued.JWT,
		"token_fingerprint": issued.Fingerprint, "token_issued_at": formatTime(issued.IssuedAt),
	}, nil)
}

func (s *Service) remove(ctx context.Context, o operator, env envelope) broker.Reply {
	e, rep := s.target(ctx, "delete", o, env)
	if rep != nil {
		return *rep
	}
	if err := s.revoke(ctx, e.UserPublicKey); err != nil {
		return s.rejectAudited(ctx, "delete", o, e.ExecutorID, err)
	}
	now := s.now()
	err := s.d.Store.InTx(ctx, func(tx store.Tx) error {
		if err := tx.DeleteExecutor(ctx, e.ExecutorID); err != nil {
			return err
		}
		if err := auditTx(ctx, tx, o, audit{action: "executor_deleted", resourceID: e.ExecutorID, outcome: "succeeded"}, now); err != nil {
			return err
		}
		return event(ctx, tx, "executor_removed", e.TeamID, o.request, now, map[string]any{
			"executor_id": e.ExecutorID, "execution_tag": e.Tag, "executor_type": e.Type,
		})
	})
	if errors.Is(err, store.ErrNotFound) {
		return s.rejectAudited(ctx, "delete", o, 0, errUnknown)
	}
	if err != nil {
		s.logFailure("delete", o, err)
		return s.rejectAudited(ctx, "delete", o, e.ExecutorID, errUnavailable)
	}
	return jsonReply(204, nil, nil)
}
