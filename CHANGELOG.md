# Changelog

All notable changes to this project are listed here. The format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/).

## [Unreleased]

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

### Changed

- The Extract service no longer has Postgres-only settings (`SourceHost`, `SourcePort`, `SourceDatabase`); they are now fields inside `ConnectorConfig`.
- Error messages hide the values of the fields the connector's spec marks as secret, instead of guessing from field names.
- When a read times out or is cancelled, only that read's connector container is stopped. Before, every container of the same connector image was stopped, which would break reads running in parallel.
- Reads that were running when the worker restarted are now marked as failed instead of staying "running" forever.
- `POST /reads` still waits for the full result by default (`wait=true`), so existing callers keep working.

## [0.0.1] - 2026-11-13

- Working prototype: IRIS production loads Postgres data through the Airbyte source-postgres connector (PyAirbyte in a separate worker). Insert/update/unchanged by key, no duplicates on rerun, source state saved after commit. Demo walkthrough in DEMO.md.
