// Package natsbroker implements internal/broker on NATS and JetStream.
package natsbroker

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"

	"github.com/pandops/flowai-executor-registry/internal/broker"
	"github.com/pandops/flowai-executor-registry/internal/config"
)

// Status is a connection's state as its handlers report it; readiness
// reads it.
type Status struct{ up atomic.Bool }

// NewStatus returns a status that is disconnected until the first connect.
func NewStatus() *Status { return &Status{} }

// Connected reports whether the connection is up.
func (s *Status) Connected() bool { return s.up.Load() }

func (s *Status) options() []nats.Option {
	return []nats.Option{
		nats.ConnectHandler(func(*nats.Conn) { s.up.Store(true) }),
		nats.ReconnectHandler(func(*nats.Conn) { s.up.Store(true) }),
		nats.DisconnectErrHandler(func(*nats.Conn, error) { s.up.Store(false) }),
		nats.ClosedHandler(func(*nats.Conn) { s.up.Store(false) }),
	}
}

// Options returns the NATS connection options for the service user:
// TLS with the configured CA, the user credentials file, the service's
// inbox prefix and endless reconnects. The connection handlers drive status.
func Options(c config.NATS, status *Status) ([]nats.Option, error) {
	if status == nil {
		return nil, errors.New("natsbroker: status is required")
	}
	if c.CredentialsFile == "" {
		return nil, errors.New("natsbroker: credentials file is required")
	}
	if c.CAFile == "" {
		return nil, errors.New("natsbroker: CA file is required")
	}
	if c.InboxPrefix == "" {
		return nil, errors.New("natsbroker: inbox prefix is required")
	}
	return append(status.options(),
		nats.Name("executor-registry"),
		nats.UserCredentials(c.CredentialsFile),
		nats.RootCAs(c.CAFile),
		nats.CustomInboxPrefix(c.InboxPrefix),
		nats.RetryOnFailedConnect(true),
		nats.MaxReconnects(-1),
		nats.ReconnectWait(time.Second),
	), nil
}

// SystemOptions returns the options of the system-account connection that
// looks up and updates the application account JWT. Its system user may
// subscribe only to the service's own reply inboxes, so it uses the same
// inbox prefix as the service connection (a different account, no clash).
func SystemOptions(c config.NATS, systemCredentialsFile string, status *Status) ([]nats.Option, error) {
	if status == nil {
		return nil, errors.New("natsbroker: status is required")
	}
	if systemCredentialsFile == "" || c.CAFile == "" {
		return nil, errors.New("natsbroker: system credentials and CA file are required")
	}
	if c.InboxPrefix == "" {
		return nil, errors.New("natsbroker: inbox prefix is required")
	}
	return append(status.options(),
		nats.Name("executor-registry-system"),
		nats.UserCredentials(systemCredentialsFile),
		nats.RootCAs(c.CAFile),
		nats.CustomInboxPrefix(c.InboxPrefix),
		nats.RetryOnFailedConnect(true),
		nats.MaxReconnects(-1),
		nats.ReconnectWait(time.Second),
	), nil
}

// Client is the broker boundary over one service connection and one
// system-account connection (account JWT look-up and update).
type Client struct {
	nc  *nats.Conn
	sys *nats.Conn
	js  jetstream.JetStream
}

var (
	_ broker.Requester       = (*Client)(nil)
	_ broker.Publisher       = (*Client)(nil)
	_ broker.ConsumerManager = (*Client)(nil)
	_ broker.AccountClient   = (*Client)(nil)
)

// Connect opens the service and the system-account connection with the
// given options. With RetryOnFailedConnect the call returns while the
// server is still unreachable; readiness reports it.
func Connect(url string, service, system []nats.Option) (*Client, error) {
	nc, err := nats.Connect(url, service...)
	if err != nil {
		return nil, fmt.Errorf("natsbroker: connect: %w", err)
	}
	sys, err := nats.Connect(url, system...)
	if err != nil {
		nc.Close()
		return nil, fmt.Errorf("natsbroker: connect system account: %w", err)
	}
	js, err := jetstream.New(nc)
	if err != nil {
		nc.Close()
		sys.Close()
		return nil, err
	}
	return &Client{nc: nc, sys: sys, js: js}, nil
}

// Conn is the service connection (for subscriptions).
func (c *Client) Conn() *nats.Conn { return c.nc }

// Close drains the service connection and closes the system connection.
func (c *Client) Close() {
	_ = c.nc.Drain()
	c.sys.Close()
}

func mapError(nc *nats.Conn, err error) error {
	switch {
	case err == nil:
		return nil
	case errors.Is(err, nats.ErrNoResponders):
		return broker.ErrNoResponders
	case !nc.IsConnected(), errors.Is(err, nats.ErrConnectionClosed), errors.Is(err, nats.ErrConnectionDraining),
		errors.Is(err, nats.ErrConnectionReconnecting), errors.Is(err, nats.ErrDisconnected):
		return fmt.Errorf("%w: %w", broker.ErrUnavailable, err)
	case errors.Is(err, nats.ErrTimeout), errors.Is(err, context.DeadlineExceeded):
		return broker.ErrTimeout
	}
	return err
}

// Request sends a core NATS request and returns its status and body.
func (c *Client) Request(ctx context.Context, subject string, header broker.Header, body []byte) (broker.Reply, error) {
	msg := nats.NewMsg(subject)
	for k, v := range header {
		msg.Header[k] = append([]string(nil), v...)
	}
	msg.Data = body
	resp, err := c.nc.RequestMsgWithContext(ctx, msg)
	if err != nil {
		return broker.Reply{}, mapError(c.nc, err)
	}
	status, err := strconv.Atoi(resp.Header.Get("status"))
	if err != nil {
		return broker.Reply{}, errors.New("natsbroker: reply without status header")
	}
	return broker.Reply{Status: status, Header: resp.Header, Body: resp.Data}, nil
}

// PublishStream publishes one stream message and waits for the JetStream
// acknowledgement; Nats-Msg-Id travels in the header.
func (c *Client) PublishStream(ctx context.Context, m broker.Message) error {
	msg := nats.NewMsg(m.Subject)
	for k, v := range m.Header {
		msg.Header[k] = append([]string(nil), v...)
	}
	msg.Data = m.Data
	pctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	_, err := c.js.PublishMsg(pctx, msg)
	return mapError(c.nc, err)
}

// CreateTaskConsumer creates the durable pull consumer through the
// JetStream API; an existing consumer of that name is kept unchanged.
func (c *Client) CreateTaskConsumer(ctx context.Context, tc broker.TaskConsumer) error {
	_, err := c.js.CreateConsumer(ctx, tc.Stream, jetstream.ConsumerConfig{
		Durable:       tc.Durable,
		FilterSubject: tc.FilterSubject,
		AckPolicy:     jetstream.AckExplicitPolicy,
		DeliverPolicy: jetstream.DeliverAllPolicy,
		AckWait:       30 * time.Second,
		MaxDeliver:    -1,
	})
	if errors.Is(err, jetstream.ErrConsumerExists) {
		return nil
	}
	return mapError(c.nc, err)
}

// LookupAccountJWT returns the live account JWT the server holds.
func (c *Client) LookupAccountJWT(ctx context.Context, accountPublicKey string) (string, error) {
	resp, err := c.sys.RequestWithContext(ctx, "$SYS.REQ.ACCOUNT."+accountPublicKey+".CLAIMS.LOOKUP", nil)
	if err != nil {
		return "", mapError(c.sys, err)
	}
	tok := strings.TrimSpace(string(resp.Data))
	if !strings.HasPrefix(tok, "eyJ") {
		return "", errors.New("natsbroker: account lookup returned no JWT")
	}
	return tok, nil
}

// UpdateAccountJWT pushes accountJWT to the running server.
func (c *Client) UpdateAccountJWT(ctx context.Context, accountJWT string) error {
	resp, err := c.sys.RequestWithContext(ctx, "$SYS.REQ.CLAIMS.UPDATE", []byte(accountJWT))
	if err != nil {
		return mapError(c.sys, err)
	}
	var r struct {
		Error *struct {
			Code        int    `json:"code"`
			Description string `json:"description"`
		} `json:"error"`
	}
	if err := json.Unmarshal(resp.Data, &r); err != nil {
		return errors.New("natsbroker: unreadable account update reply")
	}
	if r.Error != nil {
		return fmt.Errorf("natsbroker: account update refused: %d %s", r.Error.Code, r.Error.Description)
	}
	return nil
}
