// Command executor-registry runs the executor-registry service.
package main

import (
	"context"
	"crypto/rand"
	"encoding/binary"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/nats-io/nats.go"

	"github.com/pandops/flowai-executor-registry/internal/app"
	"github.com/pandops/flowai-executor-registry/internal/broker"
	"github.com/pandops/flowai-executor-registry/internal/config"
	"github.com/pandops/flowai-executor-registry/internal/cursor"
	"github.com/pandops/flowai-executor-registry/internal/natsbroker"
	"github.com/pandops/flowai-executor-registry/internal/natsjwt"
	"github.com/pandops/flowai-executor-registry/internal/outbox"
	"github.com/pandops/flowai-executor-registry/internal/server"
	"github.com/pandops/flowai-executor-registry/internal/store/pgstore"
)

const (
	queueGroup    = "executor-registry"
	sweepInterval = time.Second
	relayInterval = 200 * time.Millisecond
)

func main() {
	path := flag.String("config", "/etc/flowai/config.yaml", "configuration file")
	flag.Parse()
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil)).With("service", app.ServiceName)
	if err := run(*path, logger); err != nil {
		logger.Error("executor-registry stopped", "error", err.Error())
		os.Exit(1)
	}
}

func readTrimmed(path string) ([]byte, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read %s: %w", path, errors.Unwrap(err))
	}
	return []byte(strings.TrimSpace(string(b))), nil
}

func loadKeys(k config.NATSKeys) (natsjwt.Keys, error) {
	var out natsjwt.Keys
	pub, err := readTrimmed(k.AccountPublicKeyFile)
	if err != nil {
		return out, err
	}
	acc, err := readTrimmed(k.AccountJWTFile)
	if err != nil {
		return out, err
	}
	if out.AccountSigningSeed, err = readTrimmed(k.AccountSigningKeyFile); err != nil {
		return out, err
	}
	if out.OperatorSigningSeed, err = readTrimmed(k.OperatorSigningKeyFile); err != nil {
		return out, err
	}
	out.AccountPublicKey, out.InitialAccountJWT = string(pub), string(acc)
	return out, nil
}

func requestID() int64 {
	var b [8]byte
	_, _ = rand.Read(b[:])
	return int64(binary.BigEndian.Uint64(b[:])>>1) | 1
}

func run(path string, logger *slog.Logger) error {
	cfg, err := config.Load(path)
	if err != nil {
		return err
	}
	keys, err := loadKeys(cfg.NATSKeys)
	if err != nil {
		return err
	}
	signer, err := natsjwt.NewSigner(keys)
	if err != nil {
		return err
	}
	var ring cursor.Keyring
	idsList, secrets := cfg.CursorSecrets()
	for i := range idsList {
		ring.Keys = append(ring.Keys, cursor.Key{ID: idsList[i], Secret: secrets[i]})
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	st, err := pgstore.Open(ctx, cfg.DB.DSN(), cfg.DB.Schema)
	if err != nil {
		return err
	}
	defer st.Close()
	for {
		if err = st.Migrate(ctx); err == nil {
			break
		}
		logger.Warn("database_not_ready", "error", err.Error())
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(2 * time.Second):
		}
	}

	w, err := server.Wire(cfg, st)
	if err != nil {
		return err
	}
	bc, err := natsbroker.Connect(cfg.NATS.URL, w.Service, w.System)
	if err != nil {
		return err
	}
	defer bc.Close()

	svc := app.New(app.Deps{
		Store: st, Requester: bc, Consumers: bc, Accounts: bc, Signer: signer, Cursors: ring,
		Logger: logger, Now: time.Now, RequestID: requestID, ExecutorTypes: cfg.ExecutorTypes,
		RequestTimeout: cfg.NATS.RequestTimeout,
	})

	for _, subject := range app.Subscriptions() {
		if _, err := bc.Conn().QueueSubscribe(subject, queueGroup, func(m *nats.Msg) {
			if m.Reply == "" {
				return
			}
			rep, ok := svc.Handle(ctx, broker.Request{Subject: m.Subject, Header: m.Header, Data: m.Data})
			if !ok {
				return
			}
			out := nats.NewMsg(m.Reply)
			for k, v := range rep.Header {
				out.Header[k] = v
			}
			out.Data = rep.Body
			if err := m.RespondMsg(out); err != nil {
				logger.Warn("reply_failed", "subject", m.Subject, "error", err.Error())
			}
		}); err != nil {
			return fmt.Errorf("subscribe %s: %w", subject, err)
		}
	}

	go every(ctx, sweepInterval, func() {
		if err := svc.Sweep(ctx); err != nil && ctx.Err() == nil {
			logger.Warn("sweep_failed", "error", err.Error())
		}
	})
	relay := &outbox.Relay{Store: st, Publisher: bc}
	go every(ctx, relayInterval, func() {
		if _, err := relay.RunOnce(ctx); err != nil && ctx.Err() == nil {
			logger.Warn("outbox_relay_failed", "error", err.Error())
		}
	})

	srv := &http.Server{Addr: cfg.HTTP.Listen, Handler: w.Handler, ReadHeaderTimeout: 5 * time.Second}
	go func() {
		<-ctx.Done()
		sctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_ = srv.Shutdown(sctx)
	}()
	logger.Info("executor-registry started", "listen", cfg.HTTP.Listen)
	if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		return err
	}
	return nil
}

func every(ctx context.Context, d time.Duration, fn func()) {
	t := time.NewTicker(d)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			fn()
		}
	}
}
