package app

import (
	"bytes"
	"context"
	"encoding/json"
	"strconv"
	"time"

	"github.com/pandops/flowai-executor-registry/internal/broker"
	"github.com/pandops/flowai-executor-registry/internal/ids"
	"github.com/pandops/flowai-executor-registry/internal/store"
)

// envelope is the rule-9 request body {"params":{...},"body":...}.
type envelope struct {
	Params map[string]json.RawMessage `json:"params"`
	Body   json.RawMessage            `json:"body"`
}

func parseEnvelope(data []byte) (envelope, error) {
	var e envelope
	dec := json.NewDecoder(bytes.NewReader(data))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&e); err != nil {
		return envelope{}, errInvalid
	}
	if dec.More() {
		return envelope{}, errInvalid
	}
	if e.Params == nil {
		e.Params = map[string]json.RawMessage{}
	}
	return e, nil
}

// allowOnly rejects any parameter outside names.
func (e envelope) allowOnly(names ...string) error {
	for k := range e.Params {
		ok := false
		for _, n := range names {
			ok = ok || k == n
		}
		if !ok {
			return errInvalid
		}
	}
	return nil
}

// id reads an identifier parameter: only a JSON number token of a
// positive decimal integer within int64 is accepted.
func (e envelope) id(name string) (int64, error) {
	raw, ok := e.Params[name]
	if !ok {
		return 0, errInvalid
	}
	n, ok := ids.Parse(string(bytes.TrimSpace(raw)))
	if !ok {
		return 0, errInvalid
	}
	return n, nil
}

// page reads the optional cursor and limit parameters.
func (e envelope) page() (string, int, error) {
	limit := defaultLimit
	if raw, ok := e.Params["limit"]; ok {
		n, ok := ids.Parse(string(bytes.TrimSpace(raw)))
		if !ok || n > maxLimit {
			return "", 0, errPagination
		}
		limit = int(n)
	}
	var cur string
	if raw, ok := e.Params["cursor"]; ok {
		if err := json.Unmarshal(raw, &cur); err != nil || cur == "" || len(cur) > 1536 {
			return "", 0, errPagination
		}
	}
	return cur, limit, nil
}

func (e envelope) hasBody() bool {
	b := bytes.TrimSpace(e.Body)
	return len(b) > 0 && !bytes.Equal(b, []byte("null"))
}

func jsonReply(status int, body any, extra broker.Header) broker.Reply {
	h := broker.Header{"status": {strconv.Itoa(status)}}
	for k, v := range extra {
		h[k] = v
	}
	if body == nil {
		return broker.Reply{Status: status, Header: h}
	}
	data, _ := json.Marshal(body)
	return broker.Reply{Status: status, Header: h, Body: data}
}

// publish writes one stream message into the outbox of tx (rule 10).
func publish(ctx context.Context, tx store.Tx, subject string, teamID, requestID int64, at time.Time, fields map[string]any) error {
	seq, err := tx.NextPublisherSeq(ctx)
	if err != nil {
		return err
	}
	body := map[string]any{
		"publisher":     ServiceName,
		"publisher_seq": seq,
		"team_id":       teamID,
		"occurred_at":   formatTime(at),
	}
	for k, v := range fields {
		body[k] = v
	}
	data, err := json.Marshal(body)
	if err != nil {
		return err
	}
	header := map[string]string{"team": i64(teamID), "Nats-Msg-Id": ServiceName + "-" + i64(seq)}
	if requestID > 0 {
		header["request_id"] = i64(requestID)
	}
	return tx.InsertOutbox(ctx, store.OutboxRow{Seq: seq, Subject: subject, Header: header, Data: data})
}

func event(ctx context.Context, tx store.Tx, kind string, teamID, requestID int64, at time.Time, fields map[string]any) error {
	fields["kind"] = kind
	return publish(ctx, tx, ServiceName+".event."+kind+".pub", teamID, requestID, at, fields)
}
