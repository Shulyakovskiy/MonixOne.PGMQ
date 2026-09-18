# PGMQ 1.13.0 deployment scripts

These scripts pin PGMQ to upstream tag `v1.13.0` from `https://github.com/pgmq/pgmq.git` (release commit `32c075b`). The package also contains `pgmq-v1.13.0.tar.gz`, the official source archive for that tag. Its SHA-256 is recorded in `pgmq.lock.json`.

Extract the source archive on the target PostgreSQL host, enter `pgmq-1.13.0/pgmq-extension`, and run `make install PG_CONFIG=/path/to/pg_config` for the target server. This prepares the extension control and SQL files for that PostgreSQL installation; application runtime deliberately never executes `CREATE EXTENSION`.

Run `001-create-metadata.sql`, then `002-install-pgmq.sql`, using the deployment role. The application role needs only PGMQ queue/function privileges and `INSERT`/`UPDATE` on `monixone_queue.infrastructure_metadata`; it has no DDL privilege.

For an upgrade, create a new sibling version directory rather than editing these files, pin the new tag and full SHA in `pgmq.lock.json`, apply the vendor-provided extension upgrade path, validate it in integration tests, and only then change the supported application version.
