# Changelog

All notable changes to this project are listed here. The format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/).

## [0.1.0] - 2026-11-20

### Added

- Reads can run in the background: `POST /reads` with `wait=false` answers at once (HTTP 202) with a read id, so the caller no longer waits 25-35 seconds for the whole read.
- Progress while a read runs: `GET /reads/{id}` shows records read so far per stream, rejected-key counts and how many batches are ready.
- Batches can be fetched as soon as they are written, also while the read is still running. A batch that is not written yet answers `409 not_ready`; a batch beyond the end of a finished read answers `404`.
- `DELETE /reads/{id}` stops a running read and its connector container. A cancelled read has no state to save.
- One active read per connection: pass a `connection_id` and a second read for the same connection is refused with `409 busy`.
- A limit on parallel work in the worker (`MAX_CONCURRENT_READS`, default 2). Reads, checks and discovery all count towards it.
- `POST /spec` returns a connector's settings as JSON Schema, plus the list of secret fields. It is cached per connector image, so only the first call starts the connector.
- The worker checks the config against the connector's spec before check, discover and read. A wrong config is a `400` that lists the fields and reasons, never the values.
- `ALLOWED_CONNECTORS` (comma separated, default `source-postgres`) sets which connectors the worker runs.
- IRIS settings for any connector: `ConnectorName`, `ConnectorVersion`, `ConnectorConfig` (JSON without secrets) and `CredentialFields` (which config fields take their value from the IRIS Credentials entry, nested paths such as `tunnel_method.ssh_key=Password` allowed). A warning is logged if `ConnectorConfig` holds a value for a secret field.
- Terminal helpers `Airbyte.Demo.Util.Spec()`, `Check()` and `Discover()` show the connector's settings, validate the connection and list the streams from IRIS.
- First IRIS unit tests (`Airbyte.Test.BuildConfigTest`); the documented `%UnitTest.Manager` command now works in the container.
- IRIS runs use async reads: the Extract service polls the worker and stores each batch as soon as it is ready, while the worker is still reading. Batches of one connection are stored strictly in order; different connections run in parallel (a disabled second item `Extract2` is in the demo production).
- Retries with backoff for temporary errors (worker unreachable, HTTP 5xx, connection, timeout, internal): the whole read repeats from the last saved state, up to `MaxRetries` (default 3) with `BackoffSeconds` 5, 10, 20. New settings `PollInterval`, `HttpTimeout`, `MaxRetries`, `BackoffSeconds`.
- Configuration problems (wrong credentials, invalid config) stop a run at once with status `ConfigError` and a message that says what to fix. The Run table has new columns `RetryCount` and `ErrorCategory`.
- `CredentialFields` can name a different credentials entry per field (`credentials.client_secret=SalesforceApp.Password`), for connectors with more than two secrets.

- Run history: counts per run and per stream (`RunStream`), run duration, and a `ConnectionStatus` row per connection with the last run, the last successful load and the last error. One Event Log line per run. New helpers `Util.RunDetail(runId)` and `Util.Connections()`.
- Rejected records: a record with a missing or invalid key is stored in a new `Rejected` table (reason and record data) in the same transaction as its batch, instead of only being counted. `Util.Rejects()` lists them and `Util.IgnoreRejected(id)` marks one as ignored. New `KeyFields` setting (`stream=col1+col2`) for streams without a usable primary key.
- Data minimisation: the Store setting `ClearBatchBodies` (default on) empties the record data in the stored batch messages once a batch is committed; headers and counts stay for Visual Trace. After the state is saved, IRIS acknowledges the read and the worker deletes its batch files (`POST /reads/{id}/ack`).

- Benchmark in `bench/`: scripts that create and change a test table in Postgres, a runner for ten scenarios (initial loads of 10,000, 100,000 and one million rows, mixed changes, unchanged data, recovery after a cancelled read, ten runs in a row, three batch sizes), the raw results as CSV and a written report with the first local results (`bench/REPORT.md`).
- `Util.ResetConnection(connectionId)` clears the records, runs, rejected records, status and saved state of one connection and leaves the others alone. A disabled `ExtractBench` item in the demo production reads the benchmark table.
- The Run table has a new column `StoreSeconds` (time spent storing batches). The worker reports `bytes_produced` for a read. `RunNow` accepts a batch size and a mode for one run.

- Faster store: the worker sends a hash of each record and IRIS only compares it, and an unchanged record is found through the index without opening it. Storing 100,000 new records takes about a third less time in IRIS (see `bench/REPORT.md`).
- Shorter run start-up: the worker reuses the discovered stream list of a connector instead of starting the connector once more for every read (`CATALOG_CACHE_TTL_SECONDS`, default 3600, `0` = off). A run that reads almost nothing is about 6 to 8 seconds faster.
- Batch files are deleted as soon as IRIS has committed them (`POST /reads/{id}/batches/{n}/ack`), so the worker's read folder stays small on large reads. Batch files of a failed or cancelled read are deleted when the read ends.
- The read status shows `timings` per phase (process start, discover, connector start, connector read).
- Debug switch to time each phase of the store (`^Airbyte.Demo.ProfileOn`), off by default.

### Changed

- `ReadTimeout` now limits the whole run (default 600 s); each HTTP call to the worker has its own 30 s timeout. The Store operation has two jobs.
- The Extract service no longer has Postgres-only settings (`SourceHost`, `SourcePort`, `SourceDatabase`); they are now fields inside `ConnectorConfig`.
- Error messages hide the values of the fields the connector's spec marks as secret, instead of guessing from field names.
- When a read times out or is cancelled, only that read's connector container is stopped. Before, every container of the same connector image was stopped, which would break reads running in parallel.
- Reads that were running when the worker restarted are now marked as failed instead of staying "running" forever.
- `POST /reads` still waits for the full result by default (`wait=true`), so existing callers keep working.

### Fixed

- `Util.Reset()` left the record data of the removed records in IRIS storage (it clears the tables but not their stream data). It now clears that data as well.
- The worker's `duration_seconds` for a finished read now includes starting the read process; the figure from inside the process is kept as `read_seconds`.
- Config, catalog and state files that hold credentials were left in the worker's temp folder when a read was cancelled or timed out. Each job now has its own temp folder that is always removed, and the worker removes leftovers at startup.
- A wrong password is now reported as an authentication error. Before, the worker only said "Connector failed" because the real reason was in the connector's log file; the worker now reads it from there.

## [0.0.1] - 2026-11-13

- Working prototype: IRIS production loads Postgres data through the Airbyte source-postgres connector (PyAirbyte in a separate worker). Insert/update/unchanged by key, no duplicates on rerun, source state saved after commit. Demo walkthrough in DEMO.md.
