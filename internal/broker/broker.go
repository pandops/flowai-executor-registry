// Package broker defines executor-registry's messaging boundary: the
// request/reply, stream publish, consumer and account operations it needs
// from NATS. The NATS implementation lives in internal/natsbroker; tests
// use the in-process fake in internal/mocks.
package broker

import (
	"context"
	"errors"

	"github.com/nats-io/nats.go"
)

// Header is the NATS header map.
type Header = nats.Header

// Request is one incoming core NATS request on a `.sub` subject.
type Request struct {
	Subject string
	Header  Header
	Data    []byte
}

// Reply is the answer to a request. Status travels as the NATS reply
// header `status` (coordinator rule 9).
type Reply struct {
	Status int
	Header Header
	Body   []byte
}

// Message is one stream message as the outbox relay publishes it.
type Message struct {
	Subject string
	Header  Header
	Data    []byte
}

// TaskConsumer describes the durable pull consumer of one (team, tag).
type TaskConsumer struct {
	Stream        string
	Durable       string
	FilterSubject string
}

var (
	// ErrNoResponders: nobody serves the subject.
	ErrNoResponders = errors.New("broker: no responders")
	// ErrTimeout: no reply within the request timeout.
	ErrTimeout = errors.New("broker: request timeout")
	// ErrUnavailable: the service's own broker connection is down.
	ErrUnavailable = errors.New("broker: connection unavailable")
)

// Requester sends core NATS requests to other services.
type Requester interface {
	Request(ctx context.Context, subject string, header Header, body []byte) (Reply, error)
}

// Publisher publishes one stream message and waits for the JetStream ack.
type Publisher interface {
	PublishStream(ctx context.Context, msg Message) error
}

// ConsumerManager creates task consumers through the JetStream API on the
// running server. It deliberately has no update or delete operation.
type ConsumerManager interface {
	CreateTaskConsumer(ctx context.Context, c TaskConsumer) error
}

// AccountClient reads and pushes the application account JWT through the
// system-account user (resolver: full).
type AccountClient interface {
	LookupAccountJWT(ctx context.Context, accountPublicKey string) (string, error)
	UpdateAccountJWT(ctx context.Context, accountJWT string) error
}
