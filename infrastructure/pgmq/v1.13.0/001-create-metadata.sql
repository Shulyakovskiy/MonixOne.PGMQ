-- Executed by the package at application startup under a transaction-scoped advisory lock.
CREATE SCHEMA IF NOT EXISTS monixone_queue;

CREATE TABLE IF NOT EXISTS monixone_queue.infrastructure_metadata (
    component text PRIMARY KEY,
    status text NOT NULL,
    expected_version text NOT NULL,
    installed_version text NULL,
    source_url text NOT NULL,
    source_revision text NOT NULL,
    checked_at timestamptz NOT NULL
);

CREATE TABLE IF NOT EXISTS monixone_queue.idempotency_keys (
    consumer_name text NOT NULL,
    idempotency_key text NOT NULL CHECK (length(idempotency_key) BETWEEN 1 AND 512),
    status text NOT NULL CHECK (status IN ('processing', 'completed')),
    lease_token uuid NULL,
    lease_expires_at timestamptz NULL,
    completed_at timestamptz NULL,
    created_at timestamptz NOT NULL,
    updated_at timestamptz NOT NULL,
    PRIMARY KEY (consumer_name, idempotency_key),
    CHECK (
        (status = 'processing' AND lease_token IS NOT NULL AND lease_expires_at IS NOT NULL AND completed_at IS NULL)
        OR
        (status = 'completed' AND lease_token IS NULL AND lease_expires_at IS NULL AND completed_at IS NOT NULL)
    )
);

CREATE INDEX IF NOT EXISTS ix_idempotency_keys_processing_lease
    ON monixone_queue.idempotency_keys (lease_expires_at)
    WHERE status = 'processing';

CREATE INDEX IF NOT EXISTS ix_idempotency_keys_completed_at
    ON monixone_queue.idempotency_keys (completed_at)
    WHERE status = 'completed';

-- The application role only writes the status; it does not own this schema or table.
GRANT USAGE ON SCHEMA monixone_queue TO CURRENT_USER;
GRANT SELECT, INSERT, UPDATE ON monixone_queue.infrastructure_metadata TO CURRENT_USER;
GRANT SELECT, INSERT, UPDATE, DELETE ON monixone_queue.idempotency_keys TO CURRENT_USER;
