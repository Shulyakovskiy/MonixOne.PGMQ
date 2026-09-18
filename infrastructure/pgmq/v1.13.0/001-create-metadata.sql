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

-- The application role only writes the status; it does not own this schema or table.
GRANT USAGE ON SCHEMA monixone_queue TO CURRENT_USER;
GRANT SELECT, INSERT, UPDATE ON monixone_queue.infrastructure_metadata TO CURRENT_USER;
