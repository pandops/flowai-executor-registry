# executor-registry

Owns team Executors: their integer `executor_id` (one global sequence, never
reused), execution tag and type, the one-time NATS user JWT, the
`(team, tag)` task consumer, Executor reads, audit of operator actions,
Executor heartbeats and the connection view. Spec:
`openspec/changes/v0048-split-state-registry/specs/executor-registry/`.

## Interfaces

Served over core NATS request/reply (queue group `executor-registry`):

| Subject                                        | Caller         |
| ---------------------------------------------- | -------------- |
| `executor-registry.ui.create.sub`              | API Gateway    |
| `executor-registry.ui.list.sub`                | API Gateway    |
| `executor-registry.ui.get.sub`                 | API Gateway    |
| `executor-registry.ui.reissue.sub`             | API Gateway    |
| `executor-registry.ui.delete.sub`              | API Gateway    |
| `executor-registry.ui.list-execution-tags.sub` | API Gateway    |
| `executor-registry.service.executor-type.sub`  | model-registry |
| `executor-registry.service.team-tags.sub`      | flow-registry  |
| `executor-registry.executor.*.heartbeat.sub`   | each Executor  |

Requests it sends: `team-registry.service.get.pub` (team check before a
create) and `task-manager.service.executor-claims.pub` (after each
heartbeat). Account JWT look-up and update go through the system account
(`$SYS.REQ.ACCOUNT.<account>.CLAIMS.LOOKUP`, `$SYS.REQ.CLAIMS.UPDATE`); a
failed revocation push is never retried and answers `503`.

Stream messages (`executor-registry.event.*.pub`, `executor-registry.audit.*.pub`)
are written to the outbox in the business transaction and relayed to
JetStream with `Nats-Msg-Id = executor-registry-<publisher_seq>`. Their JSON
Schemas, and those of every request and reply, are in `schemas/`.
executor-registry consumes no stream (`schemas/consumers.json` is `[]`).

The JWT is returned only in the create and reissue replies. Only its user
public key, SHA-256 fingerprint and issue time are stored; no JWT or seed is
logged or published.

## Runtime

- Image: `Containerfile`;
  `ENTRYPOINT ["/executor-registry", "--config", "/etc/flowai/config.yaml"]`.
- Configuration: `config.example.yaml` lists every key. There are no
  environment variables.
- HTTP: one listener `:8080` with `/v1/livez` and `/v1/readyz` (ready while
  PostgreSQL answers and both NATS connections are up).
- PostgreSQL: only `executor_registry_schema`. The migration checks
  `pg_namespace` and runs `CREATE SCHEMA` only when the schema is missing.

## Tests

```sh
# FLOWAI_CONTRACT_FIXTURE_DIR points to the staged declarations and publisher schemas.
go test -mod=readonly -count=1 -coverpkg=./... -v ./...
go test -mod=readonly -count=1 -tags=integration -v ./test
```

Unit tests use the in-memory store and broker fakes in `internal/mocks/`;
integration tests (`//go:build integration`) run against PostgreSQL in a
container.
