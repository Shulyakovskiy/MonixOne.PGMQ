-- PGMQ source: https://github.com/pgmq/pgmq.git
-- Pinned release: v1.13.0 (commit 32c075b)
-- Run this as the database migration/deployment role, never from application runtime.
CREATE EXTENSION IF NOT EXISTS pgmq;

DO $$
BEGIN
    IF (SELECT extversion FROM pg_extension WHERE extname = 'pgmq') <> '1.13.0' THEN
        RAISE EXCEPTION 'Expected PGMQ extension version 1.13.0, found %',
            (SELECT extversion FROM pg_extension WHERE extname = 'pgmq');
    END IF;
END $$;
