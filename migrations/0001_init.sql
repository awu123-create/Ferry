-- +goose Up
CREATE TABLE endpoints (
    id                UUID PRIMARY KEY,
    name              TEXT NOT NULL,
    target_url        TEXT NOT NULL,
    signing_secret    TEXT NOT NULL DEFAULT '',
    rate_limit_qps    DOUBLE PRECISION NOT NULL DEFAULT 50 CHECK (rate_limit_qps > 0),
    max_concurrency   INT NOT NULL DEFAULT 20 CHECK (max_concurrency > 0),
    circuit_threshold INT NOT NULL DEFAULT 30 CHECK (circuit_threshold > 0),
    timeout_ms        INT NOT NULL DEFAULT 5000 CHECK (timeout_ms BETWEEN 100 AND 60000),
    enabled           BOOLEAN NOT NULL DEFAULT TRUE,
    created_at        TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at        TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE tasks (
    id              UUID PRIMARY KEY,
    endpoint_id     UUID NOT NULL REFERENCES endpoints(id),
    idempotency_key TEXT NOT NULL,
    payload         BYTEA NOT NULL,
    payload_sha256  TEXT NOT NULL,
    headers         JSONB NOT NULL DEFAULT '{}'::jsonb,
    status          TEXT NOT NULL CHECK (status IN (
                        'PENDING', 'RETRYING',
                        'SUCCEEDED', 'DEAD_LETTER', 'CANCELLED'
                    )),
    attempt         INT NOT NULL DEFAULT 0,
    max_attempts    INT NOT NULL DEFAULT 6 CHECK (max_attempts BETWEEN 1 AND 20),
    next_run_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
    deadline_at     TIMESTAMPTZ NOT NULL,
    last_error      TEXT NOT NULL DEFAULT '',
    replay_count    INT NOT NULL DEFAULT 0,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    completed_at    TIMESTAMPTZ
);

CREATE UNIQUE INDEX tasks_endpoint_idempotency_uq
    ON tasks (endpoint_id, idempotency_key);

CREATE INDEX tasks_reconcile_idx
    ON tasks (next_run_at ASC, id ASC)
    WHERE status IN ('PENDING', 'RETRYING');

CREATE INDEX tasks_dlq_idx
    ON tasks (endpoint_id, created_at DESC)
    WHERE status = 'DEAD_LETTER';

CREATE TABLE task_attempts (
    id               BIGSERIAL PRIMARY KEY,
    task_id          UUID NOT NULL REFERENCES tasks(id) ON DELETE CASCADE,
    replay_count     INT NOT NULL DEFAULT 0,
    attempt_no       INT NOT NULL,
    http_status      INT NOT NULL DEFAULT 0,
    duration_ms      INT NOT NULL DEFAULT 0,
    error_kind       TEXT NOT NULL DEFAULT '',
    error_message    TEXT NOT NULL DEFAULT '',
    response_snippet TEXT NOT NULL DEFAULT '',
    created_at       TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX task_attempts_task_round_idx
    ON task_attempts (task_id, replay_count, attempt_no);

CREATE TABLE outbox (
    id         BIGSERIAL PRIMARY KEY,
    task_id    UUID NOT NULL REFERENCES tasks(id) ON DELETE CASCADE,
    deliver_at TIMESTAMPTZ NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX outbox_deliver_idx
    ON outbox (deliver_at ASC, id ASC);

-- +goose StatementBegin
CREATE OR REPLACE FUNCTION notify_ferry_outbox() RETURNS trigger AS $$
BEGIN
    PERFORM pg_notify('ferry_outbox', '');
    RETURN NULL;
END;
$$ LANGUAGE plpgsql;
-- +goose StatementEnd

CREATE TRIGGER outbox_notify_trg
    AFTER INSERT ON outbox
    FOR EACH STATEMENT EXECUTE FUNCTION notify_ferry_outbox();

-- +goose Down
DROP TRIGGER IF EXISTS outbox_notify_trg ON outbox;
DROP FUNCTION IF EXISTS notify_ferry_outbox();
DROP TABLE IF EXISTS outbox;
DROP TABLE IF EXISTS task_attempts;
DROP TABLE IF EXISTS tasks;
DROP TABLE IF EXISTS endpoints;
