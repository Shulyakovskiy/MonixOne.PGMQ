# PGMQ 1.13.0 deployment scripts

## Bundled source

| Item | Value |
| --- | --- |
| Repository | [pgmq/pgmq](https://github.com/pgmq/pgmq) |
| Tag | `v1.13.0` |
| Release commit | `32c075b` |

`PgmqInitializer` verifies the pinned SHA-256 and reads the embedded archive locally.
No network access is required at startup.

## Startup

| Database state | Action |
| --- | --- |
| Clean database | Execute `pgmq-extension/sql/pgmq.sql` |
| Package-managed database | Read the version from `monixone_queue.infrastructure_metadata` |
| Upgrade required | Execute the missing `pgmq--X--Y.sql` transitions in order |

PGMQ is installed as a SQL schema. PostgreSQL extension registration is not used.

## Updating the bundled version

1. Embed the verified source archive for the target version.
2. Include its complete SQL migration chain.
3. Update the package target version.

A missing migration path fails the startup transaction before workers start.
