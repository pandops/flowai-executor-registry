package test

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/nats-io/jwt/v2"
	"github.com/nats-io/nkeys"
	"github.com/santhosh-tekuri/jsonschema/v6"

	"github.com/pandops/flowai-executor-registry/internal/app"
	"github.com/pandops/flowai-executor-registry/internal/broker"
	"github.com/pandops/flowai-executor-registry/internal/cursor"
	"github.com/pandops/flowai-executor-registry/internal/mocks"
	"github.com/pandops/flowai-executor-registry/internal/natsjwt"
	"github.com/pandops/flowai-executor-registry/internal/outbox"
	"github.com/pandops/flowai-executor-registry/internal/store"
)

const (
	subjTeamGet        = "team-registry.service.get.pub"
	subjExecutorClaims = "task-manager.service.executor-claims.pub"
	testOperatorID     = 41
	typeDocker         = "executor_docker_openhands"
	typeK8s            = "executor_k8s_openhands"
)

// testKeys is the NATS key set of one fake deployment.
type testKeys struct {
	operatorIdentity nkeys.KeyPair
	operatorSigning  nkeys.KeyPair
	account          nkeys.KeyPair
	accountSigning   nkeys.KeyPair
	accountPub       string
	initialAccount   string
}

func newTestKeys(t *testing.T) testKeys {
	t.Helper()
	must := func(kp nkeys.KeyPair, err error) nkeys.KeyPair {
		if err != nil {
			t.Fatal(err)
		}
		return kp
	}
	k := testKeys{
		operatorIdentity: must(nkeys.CreateOperator()),
		operatorSigning:  must(nkeys.CreateOperator()),
		account:          must(nkeys.CreateAccount()),
		accountSigning:   must(nkeys.CreateAccount()),
	}
	k.accountPub, _ = k.account.PublicKey()
	sigPub, _ := k.accountSigning.PublicKey()
	ac := jwt.NewAccountClaims(k.accountPub)
	ac.Name = "flowai"
	ac.SigningKeys.Add(sigPub)
	ac.Mappings = jwt.Mapping{}
	ac.Mappings["*.event.*.pub"] = []jwt.WeightedMapping{{Subject: "{{wildcard(1)}}.event.{{wildcard(2)}}.sub"}}
	ac.Mappings["*.executor.*.*.pub"] = []jwt.WeightedMapping{{Subject: "{{wildcard(1)}}.executor.{{wildcard(2)}}.{{wildcard(3)}}.sub"}}
	ac.Limits.DiskStorage = -1
	ac.Limits.MemoryStorage = -1
	enc, err := ac.Encode(k.operatorIdentity)
	if err != nil {
		t.Fatal(err)
	}
	k.initialAccount = enc
	return k
}

func seed(t *testing.T, kp nkeys.KeyPair) []byte {
	t.Helper()
	s, err := kp.Seed()
	if err != nil {
		t.Fatal(err)
	}
	return s
}

// syncBuffer is a goroutine-safe log sink.
type syncBuffer struct {
	mu sync.Mutex
	b  bytes.Buffer
}

func (s *syncBuffer) Write(p []byte) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.b.Write(p)
}

func (s *syncBuffer) String() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.b.String()
}

type team struct {
	archived bool
}

// harness wires executor-registry with the memory store and fake broker.
type harness struct {
	t      *testing.T
	store  *mocks.MemStore
	broker *mocks.Broker
	svc    *app.Service
	keys   testKeys
	logs   *syncBuffer
	mu     sync.Mutex
	now    time.Time
	reqID  int64
	teams  map[int64]team
	// claimsHeld, when set, is what the fake task-manager answers; nil
	// echoes every reported claim back as held.
	claimsHeld func(body map[string]any) []map[string]any
	cursorKey  []byte
}

func newHarness(t *testing.T) *harness {
	t.Helper()
	h := &harness{
		t:         t,
		store:     mocks.NewMemStore(),
		broker:    mocks.NewBroker(),
		keys:      newTestKeys(t),
		logs:      &syncBuffer{},
		now:       time.Date(2026, 10, 15, 8, 58, 0, 0, time.UTC),
		reqID:     1000,
		teams:     map[int64]team{3: {}, 4: {}},
		cursorKey: []byte("0123456789abcdef0123456789abcdef"),
	}
	h.broker.AccountJWT = h.keys.initialAccount
	h.broker.Respond(subjTeamGet, h.teamRegistry)
	h.broker.Respond(subjExecutorClaims, h.taskManager)
	h.svc = h.newService(h.store, h.broker)
	return h
}

func (h *harness) signer() *natsjwt.Signer {
	s, err := natsjwt.NewSigner(natsjwt.Keys{
		AccountPublicKey:    h.keys.accountPub,
		AccountSigningSeed:  seed(h.t, h.keys.accountSigning),
		OperatorSigningSeed: seed(h.t, h.keys.operatorSigning),
		InitialAccountJWT:   h.keys.initialAccount,
	})
	if err != nil {
		h.t.Logf("natsjwt.NewSigner: %v", err)
	}
	return s
}

func (h *harness) newService(st store.Store, b *mocks.Broker) *app.Service {
	return app.New(app.Deps{
		Store:          st,
		Requester:      b,
		Consumers:      b,
		Accounts:       b,
		Signer:         h.signer(),
		Cursors:        cursor.Keyring{Keys: []cursor.Key{{ID: 1, Secret: h.cursorKey}}},
		Logger:         slog.New(slog.NewJSONHandler(h.logs, &slog.HandlerOptions{Level: slog.LevelDebug})),
		Now:            h.clock,
		RequestID:      h.nextRequestID,
		ExecutorTypes:  []string{typeDocker, typeK8s},
		RequestTimeout: 5 * time.Second,
	})
}

func (h *harness) clock() time.Time {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.now
}

func (h *harness) advance(d time.Duration) {
	h.mu.Lock()
	h.now = h.now.Add(d)
	h.mu.Unlock()
}

func (h *harness) nextRequestID() int64 {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.reqID++
	return h.reqID
}

func (h *harness) teamRegistry(header broker.Header, body []byte) (broker.Reply, error) {
	var req struct {
		Params struct {
			TeamID int64 `json:"team_id"`
		} `json:"params"`
	}
	_ = json.Unmarshal(body, &req)
	tm, ok := h.teams[req.Params.TeamID]
	if !ok {
		return jsonReply(404, map[string]any{"code": "canonical_team_not_found", "message": "canonical team was not found", "request_id": 1}), nil
	}
	var archivedAt any
	if tm.archived {
		archivedAt = "2026-10-01T00:00:00.000Z"
	}
	return jsonReply(200, map[string]any{
		"team_id": req.Params.TeamID, "team_name": "team-" + strconv.FormatInt(req.Params.TeamID, 10),
		"default_image": "ghcr.io/flowai/agent@sha256:" + strings.Repeat("a", 64),
		"ingested_at":   "2026-09-01T00:00:00.000Z", "archived_at": archivedAt,
	}), nil
}

func (h *harness) taskManager(header broker.Header, body []byte) (broker.Reply, error) {
	var req map[string]any
	if !json.Valid(body) {
		return jsonReply(400, map[string]any{"code": "invalid_request", "message": "bad", "request_id": 1}), nil
	}
	_ = json.Unmarshal(body, &req)
	inner, _ := req["body"].(map[string]any)
	var held []map[string]any
	if h.claimsHeld != nil {
		held = h.claimsHeld(inner)
	} else {
		claims, _ := inner["claims"].([]any)
		for _, c := range claims {
			m := c.(map[string]any)
			held = append(held, map[string]any{"task_id": m["task_id"], "attempt": m["attempt"], "claim_seq": m["claim_seq"]})
		}
	}
	if held == nil {
		held = []map[string]any{}
	}
	return jsonReply(200, map[string]any{"executor_id": inner["executor_id"], "claims": held}), nil
}

func jsonReply(status int, body any) broker.Reply {
	data, _ := json.Marshal(body)
	return broker.Reply{Status: status, Header: broker.Header{"status": {strconv.Itoa(status)}}, Body: data}
}

// reply is a decoded service reply.
type reply struct {
	Status int
	OK     bool
	Header broker.Header
	Raw    []byte
	Body   map[string]any
}

func (r reply) code() string {
	c, _ := r.Body["code"].(string)
	return c
}

func (h *harness) handle(subject string, header broker.Header, data []byte) reply {
	h.t.Helper()
	rep, ok := h.svc.Handle(context.Background(), broker.Request{Subject: subject, Header: header, Data: data})
	r := reply{Status: rep.Status, OK: ok, Header: rep.Header, Raw: rep.Body}
	if len(rep.Body) > 0 {
		_ = json.Unmarshal(rep.Body, &r.Body)
	}
	if ok && rep.Header != nil {
		if s := rep.Header.Get("status"); s != strconv.Itoa(rep.Status) {
			h.t.Errorf("%s: reply header status %q does not equal status %d", subject, s, rep.Status)
		}
	}
	if ok && rep.Status >= 400 {
		h.validate("error.reply.schema.json", rep.Body)
	}
	return r
}

func uiHeader(teamID int64, requestID int64) broker.Header {
	return broker.Header{
		"team":        {strconv.FormatInt(teamID, 10)},
		"operator_id": {strconv.Itoa(testOperatorID)},
		"request_id":  {strconv.FormatInt(requestID, 10)},
		"team_name":   {"team-" + strconv.FormatInt(teamID, 10)},
	}
}

func envelope(params map[string]any, body any) []byte {
	if params == nil {
		params = map[string]any{}
	}
	m := map[string]any{"params": params}
	if body != nil {
		m["body"] = body
	}
	data, _ := json.Marshal(m)
	return data
}

// ui sends executor-registry.ui.<op>.sub as API Gateway for team.
func (h *harness) ui(op string, teamID int64, params map[string]any, body any) reply {
	h.t.Helper()
	return h.handle("executor-registry.ui."+op+".sub", uiHeader(teamID, h.nextRequestID()), envelope(params, body))
}

func (h *harness) uiRaw(op string, header broker.Header, data []byte) reply {
	h.t.Helper()
	return h.handle("executor-registry.ui."+op+".sub", header, data)
}

// create registers an Executor and returns its id and token.
func (h *harness) create(teamID int64, tag, typ string) (int64, string) {
	h.t.Helper()
	r := h.ui("create", teamID, map[string]any{"team_id": teamID}, map[string]any{"execution_tag": tag, "executor_type": typ})
	if r.Status != 201 {
		h.t.Fatalf("create(%d,%q) = %d %s", teamID, tag, r.Status, r.Raw)
	}
	h.validate("ui.create.reply.schema.json", r.Raw)
	id, _ := r.Body["executor_id"].(float64)
	tok, _ := r.Body["token"].(string)
	return int64(id), tok
}

func (h *harness) get(teamID, executorID int64) reply {
	h.t.Helper()
	return h.ui("get", teamID, map[string]any{"team_id": teamID, "executor_id": executorID}, nil)
}

func (h *harness) reissue(teamID, executorID int64) reply {
	h.t.Helper()
	return h.ui("reissue", teamID, map[string]any{"team_id": teamID, "executor_id": executorID}, nil)
}

func (h *harness) remove(teamID, executorID int64) reply {
	h.t.Helper()
	return h.ui("delete", teamID, map[string]any{"team_id": teamID, "executor_id": executorID}, nil)
}

// hb is one heartbeat body.
type hb struct {
	Claims      []map[string]any
	MaxParallel int
	Slots       int
	Interval    int
	Miss        int
	Wait        int
	IP          any // string, nil (null) or omitIP
}

type omit struct{}

var omitIP = omit{}

func defaultHB() hb {
	return hb{Claims: nil, MaxParallel: 2, Slots: 0, Interval: 15, Miss: 3, Wait: 600, IP: "192.0.2.10"}
}

func claim(task, attempt, seq int64, state string) map[string]any {
	return map[string]any{"task_id": task, "attempt": attempt, "claim_seq": seq, "state": state}
}

func (b hb) body() map[string]any {
	claims := b.Claims
	if claims == nil {
		claims = []map[string]any{}
	}
	m := map[string]any{
		"executor_type": typeDocker, "claims": claims, "max_parallel_tasks": b.MaxParallel,
		"slots_in_use": b.Slots, "interval_seconds": b.Interval, "miss_limit": b.Miss, "wait_seconds": b.Wait,
	}
	if _, skip := b.IP.(omit); !skip {
		m["ip"] = b.IP
	}
	return m
}

// heartbeat sends executor-registry.executor.<id>.heartbeat.sub. The team
// header is deliberately wrong to show it is ignored.
func (h *harness) heartbeat(executorID int64, b hb) reply {
	h.t.Helper()
	header := broker.Header{"team": {"999"}, "request_id": {strconv.FormatInt(h.nextRequestID(), 10)}}
	r := h.handle(fmt.Sprintf("executor-registry.executor.%d.heartbeat.sub", executorID), header, envelope(nil, b.body()))
	if r.OK && r.Status == 200 {
		h.validate("executor.heartbeat.reply.schema.json", r.Raw)
	}
	return r
}

func (h *harness) sweep() {
	h.t.Helper()
	if err := h.svc.Sweep(context.Background()); err != nil {
		h.t.Fatalf("sweep: %v", err)
	}
}

// outMsg is one decoded outbox row.
type outMsg struct {
	Seq     int64
	Subject string
	Header  map[string]string
	Body    map[string]any
	Raw     []byte
}

func (h *harness) outbox() []outMsg {
	h.t.Helper()
	var out []outMsg
	for _, r := range h.store.Outbox() {
		m := outMsg{Seq: r.Seq, Subject: r.Subject, Header: r.Header, Raw: r.Data}
		if err := json.Unmarshal(r.Data, &m.Body); err != nil {
			h.t.Fatalf("outbox row %d is not JSON: %v", r.Seq, err)
		}
		out = append(out, m)
	}
	return out
}

func (h *harness) events(kind string) []outMsg {
	var out []outMsg
	for _, m := range h.outbox() {
		if m.Subject == "executor-registry.event."+kind+".pub" {
			out = append(out, m)
		}
	}
	return out
}

func (h *harness) audits(action string) []outMsg {
	var out []outMsg
	for _, m := range h.outbox() {
		if m.Subject == "executor-registry.audit."+action+".pub" {
			out = append(out, m)
		}
	}
	return out
}

func (h *harness) eventsFor(kind string, executorID int64) []outMsg {
	var out []outMsg
	for _, m := range h.events(kind) {
		if num(m.Body["executor_id"]) == executorID {
			out = append(out, m)
		}
	}
	return out
}

func num(v any) int64 {
	switch x := v.(type) {
	case float64:
		return int64(x)
	case json.Number:
		n, _ := x.Int64()
		return n
	}
	return -1
}

// relay publishes pending outbox rows to the fake broker.
func (h *harness) relay() int {
	h.t.Helper()
	r := &outbox.Relay{Store: h.store, Publisher: h.broker}
	n, err := r.RunOnce(context.Background())
	if err != nil {
		h.t.Fatalf("relay: %v", err)
	}
	return n
}

// ---- JSON Schema validation against svc/executor-registry/schemas ----

var (
	schemaMu    sync.Mutex
	schemaCache = map[string]*jsonschema.Schema{}
)

func schemaDir() string { return filepath.Join("..", "schemas") }

func compileSchema(t *testing.T, path string) *jsonschema.Schema {
	t.Helper()
	schemaMu.Lock()
	defer schemaMu.Unlock()
	if s, ok := schemaCache[path]; ok {
		return s
	}
	f, err := os.Open(path)
	if err != nil {
		t.Fatalf("open schema %s: %v", path, err)
	}
	defer f.Close()
	doc, err := jsonschema.UnmarshalJSON(f)
	if err != nil {
		t.Fatalf("parse schema %s: %v", path, err)
	}
	c := jsonschema.NewCompiler()
	if err := c.AddResource(path, doc); err != nil {
		t.Fatalf("add schema %s: %v", path, err)
	}
	s, err := c.Compile(path)
	if err != nil {
		t.Fatalf("compile schema %s: %v", path, err)
	}
	schemaCache[path] = s
	return s
}

func schemaError(t *testing.T, name string, data []byte) error {
	t.Helper()
	s := compileSchema(t, filepath.Join(schemaDir(), name))
	inst, err := jsonschema.UnmarshalJSON(bytes.NewReader(data))
	if err != nil {
		return err
	}
	return s.Validate(inst)
}

func (h *harness) validate(name string, data []byte) {
	h.t.Helper()
	if err := schemaError(h.t, name, data); err != nil {
		h.t.Errorf("%s does not validate %s: %v", name, data, err)
	}
}

// schemaFor maps a published subject to its schema file.
func schemaFor(subject string) string {
	parts := strings.Split(subject, ".")
	if len(parts) == 4 && parts[0] == "executor-registry" && parts[3] == "pub" {
		switch parts[1] {
		case "event":
			return "event." + parts[2] + ".schema.json"
		case "audit":
			return "audit." + parts[2] + ".schema.json"
		}
	}
	return ""
}

// checkOutbox validates every outbox row: subject, schema, headers and
// message identity (design rules 10 and 43).
func (h *harness) checkOutbox() {
	h.t.Helper()
	var last int64
	for _, m := range h.outbox() {
		name := schemaFor(m.Subject)
		if name == "" {
			h.t.Errorf("outbox subject %q is not an executor-registry event or audit subject", m.Subject)
			continue
		}
		h.validate(name, m.Raw)
		if m.Seq <= last {
			h.t.Errorf("outbox seq %d not increasing after %d", m.Seq, last)
		}
		last = m.Seq
		if num(m.Body["publisher_seq"]) != m.Seq {
			h.t.Errorf("publisher_seq %v != outbox seq %d", m.Body["publisher_seq"], m.Seq)
		}
		if m.Body["publisher"] != "executor-registry" {
			h.t.Errorf("publisher = %v", m.Body["publisher"])
		}
		if got, want := m.Header["Nats-Msg-Id"], fmt.Sprintf("executor-registry-%d", m.Seq); got != want {
			h.t.Errorf("Nats-Msg-Id = %q, want %q", got, want)
		}
		if got, want := m.Header["team"], strconv.FormatInt(num(m.Body["team_id"]), 10); got != want {
			h.t.Errorf("%s: team header %q, want %q", m.Subject, got, want)
		}
	}
}

// ---- JWT helpers ----

func decodeUser(t *testing.T, token string) *jwt.UserClaims {
	t.Helper()
	uc, err := jwt.DecodeUserClaims(token)
	if err != nil {
		t.Fatalf("decode user JWT: %v", err)
	}
	return uc
}

func decodeAccount(t *testing.T, token string) *jwt.AccountClaims {
	t.Helper()
	ac, err := jwt.DecodeAccountClaims(token)
	if err != nil {
		t.Fatalf("decode account JWT: %v", err)
	}
	return ac
}

// subjectMatch implements NATS wildcard matching of subject against pattern.
func subjectMatch(pattern, subject string) bool {
	p := strings.Split(pattern, ".")
	s := strings.Split(subject, ".")
	for i, tok := range p {
		if tok == ">" {
			return len(s) > i
		}
		if i >= len(s) {
			return false
		}
		if tok != "*" && tok != s[i] {
			return false
		}
	}
	return len(p) == len(s)
}

func allowed(list jwt.StringList, subject string) bool {
	for _, p := range list {
		if subjectMatch(p, subject) {
			return true
		}
	}
	return false
}

func sortedStrings(in []string) []string {
	out := append([]string(nil), in...)
	sort.Strings(out)
	return out
}

func mustJSON(v any) []byte {
	data, _ := json.Marshal(v)
	return data
}

func unmarshalJSON(s string) (any, error) {
	return jsonschema.UnmarshalJSON(strings.NewReader(s))
}
