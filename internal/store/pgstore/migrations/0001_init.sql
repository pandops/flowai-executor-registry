-- executor-registry schema: Executors (no JWT, only its public key and
-- fingerprint), connection records, recorded task consumers and the outbox.
CREATE SEQUENCE executor_id_seq AS bigint START 1 NO CYCLE;
CREATE SEQUENCE publisher_seq AS bigint START 1 NO CYCLE;

CREATE TABLE executors (
    executor_id       bigint PRIMARY KEY CHECK (executor_id > 0),
    team_id           bigint NOT NULL CHECK (team_id > 0),
    execution_tag     text NOT NULL CHECK (execution_tag ~ '^[A-Za-z0-9_-]{1,128}$'),
    executor_type     text NOT NULL,
    created_at        timestamptz NOT NULL,
    user_public_key   text NOT NULL,
    token_fingerprint text NOT NULL CHECK (token_fingerprint ~ '^[0-9a-f]{64}$'),
    token_issued_at   timestamptz NOT NULL,
    reported_ip       text,
    ip_reported_at    timestamptz
);
CREATE INDEX executors_team ON executors (team_id, executor_id);
CREATE INDEX executors_team_tag ON executors (team_id, execution_tag COLLATE "C");

CREATE TABLE connections (
    executor_id       bigint PRIMARY KEY REFERENCES executors (executor_id) ON DELETE CASCADE,
    team_id           bigint NOT NULL,
    state             text NOT NULL CHECK (state IN ('never_connected', 'connected', 'connection_lost', 'executor_lost')),
    last_heartbeat_at timestamptz,
    interval_seconds  integer NOT NULL,
    miss_limit        integer NOT NULL,
    wait_seconds      integer NOT NULL,
    max_parallel      integer NOT NULL DEFAULT 0,
    slots_in_use      integer NOT NULL DEFAULT 0,
    held_claims       integer NOT NULL DEFAULT 0,
    status_published  boolean NOT NULL DEFAULT false,
    status_state      text NOT NULL DEFAULT '',
    status_max        integer NOT NULL DEFAULT 0,
    status_held       integer NOT NULL DEFAULT 0,
    status_available  integer NOT NULL DEFAULT 0,
    lost_seq          bigint NOT NULL DEFAULT 0
);
CREATE INDEX connections_tracked ON connections (state) WHERE state IN ('connected', 'connection_lost');

CREATE TABLE task_consumers (
    team_id       bigint NOT NULL,
    execution_tag text NOT NULL,
    consumer_name text NOT NULL,
    PRIMARY KEY (team_id, execution_tag)
);

CREATE TABLE outbox (
    publisher_seq bigint PRIMARY KEY,
    subject       text NOT NULL,
    header        jsonb NOT NULL,
    data          bytea NOT NULL
);
