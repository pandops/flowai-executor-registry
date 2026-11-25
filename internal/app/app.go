// Package app implements executor-registry's operations: team Executors,
// their NATS JWTs, task consumers, reads, audit, heartbeats and the
// connection record.
package app

import (
	"context"
	"errors"
	"log/slog"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/pandops/flowai-executor-registry/internal/broker"
	"github.com/pandops/flowai-executor-registry/internal/cursor"
	"github.com/pandops/flowai-executor-registry/internal/ids"
	"github.com/pandops/flowai-executor-registry/internal/natsjwt"
	"github.com/pandops/flowai-executor-registry/internal/store"
)

// ServiceName is the publisher name and subject prefix.
const ServiceName = "executor-registry"

// DefaultExecutorTypes is the supported executor_type set.
var DefaultExecutorTypes = []string{"executor_docker_openhands", "executor_k8s_openhands"}

const (
	subjTeamGet        = "team-registry.service.get.pub"
	subjExecutorClaims = "task-manager.service.executor-claims.pub"
	taskStream         = "TASK_MANAGER_TASK"
	timeFormat         = "2006-01-02T15:04:05.000Z"
	defaultLimit       = 50
	maxLimit           = 200
)

// Deps are the service's collaborators.
type Deps struct {
	Store          store.Store
	Requester      broker.Requester
	Consumers      broker.ConsumerManager
	Accounts       broker.AccountClient
	Signer         *natsjwt.Signer
	Cursors        cursor.Keyring
	Logger         *slog.Logger
	Now            func() time.Time
	RequestID      func() int64
	ExecutorTypes  []string
	RequestTimeout time.Duration
}

// Service is executor-registry.
type Service struct {
	d Deps
	// account serializes look-up, re-sign and push of the account JWT so
	// that concurrent revocations never drop each other.
	account sync.Mutex
}

// New builds the service.
func New(d Deps) *Service {
	if d.Now == nil {
		d.Now = time.Now
	}
	if d.Logger == nil {
		d.Logger = slog.Default()
	}
	if d.RequestTimeout <= 0 {
		d.RequestTimeout = 5 * time.Second
	}
	if len(d.ExecutorTypes) == 0 {
		d.ExecutorTypes = DefaultExecutorTypes
	}
	return &Service{d: d}
}

var uiOps = []string{"create", "list", "get", "reissue", "delete", "list-execution-tags"}

// Subscriptions lists the exact `.sub` subjects the service serves.
func Subscriptions() []string {
	out := []string{
		ServiceName + ".executor.*.heartbeat.sub",
		ServiceName + ".service.executor-type.sub",
		ServiceName + ".service.team-tags.sub",
	}
	for _, op := range uiOps {
		out = append(out, ServiceName+".ui."+op+".sub")
	}
	return out
}

// Handle serves one request. ok=false means no reply is sent (a heartbeat
// whose record could not be committed, or a subject the service does not serve).
func (s *Service) Handle(ctx context.Context, req broker.Request) (reply broker.Reply, ok bool) {
	parts := strings.Split(req.Subject, ".")
	if len(parts) < 4 || parts[0] != ServiceName || parts[len(parts)-1] != "sub" {
		return broker.Reply{}, false
	}
	switch {
	case len(parts) == 4 && parts[1] == "ui":
		for _, op := range uiOps {
			if op == parts[2] {
				return s.handleUI(ctx, op, req), true
			}
		}
	case len(parts) == 4 && parts[1] == "service" && parts[2] == "executor-type":
		return s.executorType(ctx, req), true
	case len(parts) == 4 && parts[1] == "service" && parts[2] == "team-tags":
		return s.teamTags(ctx, req), true
	case len(parts) == 5 && parts[1] == "executor" && parts[3] == "heartbeat":
		return s.heartbeat(ctx, parts[2], req)
	}
	return broker.Reply{}, false
}

// ---- errors ----

type apiError struct {
	status  int
	code    string
	message string
}

func (e *apiError) Error() string { return e.code }

var (
	errInvalid     = &apiError{400, "invalid_request", "request is invalid"}
	errTagCount    = &apiError{400, "invalid_tag_count", "exactly one execution_tag is required"}
	errPagination  = &apiError{400, "invalid_pagination", "cursor or limit is invalid"}
	errUnknown     = &apiError{404, "executor_unknown_or_unavailable", "executor is unknown or unavailable"}
	errTeamUnknown = &apiError{404, "canonical_team_not_found", "canonical team was not found"}
	errArchived    = &apiError{409, "team_archived", "team is archived"}
	errUnavailable = &apiError{503, "service_unavailable", "a required service is unavailable"}
	errBroker      = &apiError{503, "broker_unavailable", "message broker is unavailable"}
)

// brokerError maps a broker failure to the reply it causes.
func brokerError(err error) *apiError {
	if errors.Is(err, broker.ErrUnavailable) {
		return errBroker
	}
	return errUnavailable
}

func asAPI(err error) *apiError {
	var a *apiError
	if errors.As(err, &a) {
		return a
	}
	return errUnavailable
}

func (s *Service) errReply(err error, requestID int64) broker.Reply {
	a := asAPI(err)
	body := map[string]any{"code": a.code, "message": a.message}
	// No valid request_id header: the field is left out (D12).
	if requestID > 0 {
		body["request_id"] = requestID
	}
	return jsonReply(a.status, body, nil)
}

func formatTime(t time.Time) string { return t.UTC().Format(timeFormat) }

func (s *Service) now() time.Time { return s.d.Now().UTC() }

func (s *Service) timeout(ctx context.Context) (context.Context, context.CancelFunc) {
	return context.WithTimeout(ctx, s.d.RequestTimeout)
}

func (s *Service) supportedType(t string) bool {
	for _, x := range s.d.ExecutorTypes {
		if x == t {
			return true
		}
	}
	return false
}

func headerID(h broker.Header, key string) (int64, bool) {
	if h == nil {
		return 0, false
	}
	v := h[key]
	if len(v) != 1 {
		return 0, false
	}
	return ids.Parse(v[0])
}

func i64(n int64) string { return strconv.FormatInt(n, 10) }
