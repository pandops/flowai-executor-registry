package mocks

import (
	"context"
	"sync"

	"github.com/pandops/flowai-executor-registry/internal/broker"
)

// SentRequest is one request the service sent.
type SentRequest struct {
	Subject string
	Header  broker.Header
	Body    []byte
}

// Responder answers a request on one subject.
type Responder func(header broker.Header, body []byte) (broker.Reply, error)

// Broker is a fake of every broker boundary: requests, stream publishes,
// task-consumer creation and the system-account account JWT operations.
type Broker struct {
	mu         sync.Mutex
	responders map[string]Responder
	Requests   []SentRequest
	Published  []broker.Message
	Consumers  []broker.TaskConsumer
	// AccountJWT is the live application account JWT on the fake server.
	AccountJWT     string
	AccountLookups int
	AccountPushes  []string
	// Disconnected makes Connected report false.
	Disconnected bool
	// Fault injection.
	FailCreate        error
	FailAccountLookup error
	FailAccountUpdate error
}

// NewBroker returns a fake broker with no responders.
func NewBroker() *Broker { return &Broker{responders: map[string]Responder{}} }

// Respond installs a responder; nil removes it (no responders).
func (b *Broker) Respond(subject string, r Responder) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if r == nil {
		delete(b.responders, subject)
		return
	}
	b.responders[subject] = r
}

// RequestsTo returns the requests sent to subject.
func (b *Broker) RequestsTo(subject string) []SentRequest {
	b.mu.Lock()
	defer b.mu.Unlock()
	var out []SentRequest
	for _, r := range b.Requests {
		if r.Subject == subject {
			out = append(out, r)
		}
	}
	return out
}

func (b *Broker) Request(ctx context.Context, subject string, header broker.Header, body []byte) (broker.Reply, error) {
	b.mu.Lock()
	b.Requests = append(b.Requests, SentRequest{Subject: subject, Header: cloneHeader(header), Body: append([]byte(nil), body...)})
	r := b.responders[subject]
	b.mu.Unlock()
	if r == nil {
		return broker.Reply{}, broker.ErrNoResponders
	}
	return r(header, body)
}

func (b *Broker) PublishStream(ctx context.Context, msg broker.Message) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.Published = append(b.Published, msg)
	return nil
}

func (b *Broker) CreateTaskConsumer(ctx context.Context, c broker.TaskConsumer) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.FailCreate != nil {
		return b.FailCreate
	}
	b.Consumers = append(b.Consumers, c)
	return nil
}

func (b *Broker) LookupAccountJWT(ctx context.Context, accountPublicKey string) (string, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.AccountLookups++
	if b.FailAccountLookup != nil {
		return "", b.FailAccountLookup
	}
	return b.AccountJWT, nil
}

func (b *Broker) UpdateAccountJWT(ctx context.Context, accountJWT string) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.AccountPushes = append(b.AccountPushes, accountJWT)
	if b.FailAccountUpdate != nil {
		return b.FailAccountUpdate
	}
	b.AccountJWT = accountJWT
	return nil
}

func cloneHeader(h broker.Header) broker.Header {
	out := broker.Header{}
	for k, v := range h {
		out[k] = append([]string(nil), v...)
	}
	return out
}

// Connected reports the fake connection state.
func (b *Broker) Connected() bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	return !b.Disconnected
}
