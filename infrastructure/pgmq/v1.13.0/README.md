# PGMQ 1.13.0 deployment scripts

This package embeds the PGMQ `v1.13.0` source archive from `https://github.com/pgmq/pgmq` (release commit `32c075b`). `PgmqInitializer` reads the archive locally; no network access is performed.

For a clean database it executes `pgmq-extension/sql/pgmq.sql`. For a package-managed database it reads the applied version from `monixone_queue.infrastructure_metadata` and executes the required `pgmq--X--Y.sql` transition files in order. PostgreSQL extension registration is not used.

For a later version, embed the verified source archive with its full SQL migration chain and update the package target version. A missing migration path fails the startup transaction before application workers start.
