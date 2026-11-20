# Design notes

## Goal

Load records from Airbyte sources into InterSystems IRIS, with all setup and operation in an IRIS production. No Airbyte server, no Airbyte web UI.

## Components

| Component | Runs in | Role |
|---|---|---|
| Worker | Own container, Python 3.11 | FastAPI service around PyAirbyte. Runs the Airbyte connector, converts values, returns records in batches and the new source state |
| Airbyte connector | Docker container started by the worker | `airbyte/source-postgres`, the same image the Airbyte platform uses |
| `Airbyte.Demo.Service.Extract` | IRIS | Business Service: settings, credentials, run lock, calls the worker, sends batches, saves state |
| `Airbyte.Demo.Operation.Store` | IRIS | Business Operation: insert-or-update, one transaction per batch |
| `Airbyte.Demo.Data.Record`, `Run` | IRIS | Stored records and run counters |

Extraction runs outside IRIS on purpose: connectors bring their own runtimes (Java, Python) and Docker, which should not run inside the database process.

## Worker API

| Method | Path | Purpose |
|---|---|---|
| GET | `/health`, `/env` | Liveness; versions |
| POST | `/spec` | The connector's settings as JSON Schema (`connection_specification`) and `secret_fields` (dotted paths of every `airbyte_secret` property, also inside `oneOf` options). Cached per image in memory and under `/data/specs/` |
| POST | `/check` | Validate config and connection. A config that does not fit the spec is `400 config` with `details: [{path, reason}]` (no values) |
| POST | `/discover` | Streams, schemas, primary keys, sync modes |
| POST | `/reads` | Start a read; body carries config, streams, mode, batch size, input state, optional `connection_id` and `wait` (default `true`). `wait=true`: returns the full result when done. `wait=false`: returns `202 {read_id, status: "running"}` at once |
| GET | `/reads/{id}` | Status (`running`, `completed`, `failed`, `cancelled`) with progress: records so far per stream, key errors, `batches_ready`. Final counts, batches, sync modes and duration when completed; `error {category, message}` when failed |
| GET | `/reads/{id}/batches/{n}` | One batch of records; available as soon as it is written, also while the read runs. `409 not_ready` if not written yet and the read is running; `404` if the read has finished and has no batch n |
| GET | `/reads/{id}/state` | Source state after the read; only for completed reads |
| DELETE | `/reads/{id}` | Stop a running read (kills its process and its connector container); status becomes `cancelled`, no state. `409 not_running` for a finished read |

Record format:

```json
{"stream": "customers", "key": ["1"], "data": {"id": 1, "name": "Alice Smith", "balance": "120.50", "updated_at": "2026-11-01T10:00:00+00:00", "email": "alice@example.com"}}
```

## Connector settings

IRIS does not know which connector it talks to. The Extract service has `ConnectorName`, `ConnectorVersion`, `ConnectorConfig` (the connector's own settings as JSON, without secrets) and `CredentialFields` (`config.path=CredentialsProperty` pairs, dotted paths for nested fields). At run time `BuildConfig` puts the values from the IRIS Credentials entry into the config at those paths. The worker validates the config against the connector's spec before check, discover and read, and hides the spec's secret fields in error messages. `Util.Spec()`, `Util.Check()` and `Util.Discover()` show the spec, validate the connection and list the streams from the IRIS terminal.

## Rules the code follows

- **State after commit.** The worker returns the new source state but never treats it as committed. IRIS saves it only after every batch is committed. A failed run leaves the state unchanged, so the next run reads again from the old position.
- **No duplicates.** Identity is connection + stream + key, enforced by a unique index. Replaying a batch updates or skips, never inserts twice.
- **Changed vs unchanged.** SHA-256 over the record data with keys sorted. Same hash: counted as unchanged, not written.
- **No invented keys.** A record with a missing or empty key is rejected and counted.
- **No silent loss.** Decimals travel as strings with their scale, integers stay integers, Unicode is kept, timestamps are ISO 8601 with offset, explicit nulls are kept and absent fields are not added. `_airbyte_*` metadata is removed.
- **No deletes.** A record missing from the source is never deleted in IRIS.
- **No secrets in logs.** Credentials come from an IRIS credentials entry, travel in the request body, and are never logged or echoed in errors.

## Reads

- Each read gets its own working folder; nothing is reused between reads. The input state comes only from the request.
- The worker runs the connector and parses its output lines directly, so numbers keep their exact text.
- A read runs in a child process with a timeout (default 120 s). On timeout the process and its connector container are stopped; status becomes `failed` with category `timeout` (with `wait=true` the API answers 504).
- Batch files are written while the connector streams records, and `progress.json` is updated after each one. `GET /reads/{id}` merges `status.json` and `progress.json`; a caller can fetch batch 1 while the read is still running. The state is written only at the end of a successful read, and `committed` is always false.
- `wait=false` runs the read in a background thread of the worker. A read that was running when the worker restarted is marked `failed` (internal) at startup.
- Concurrency: at most one active read per `connection_id` (409 `busy`), and at most `MAX_CONCURRENT_READS` (default 2) operations in total; check and discover count too. Each connector container gets the label `airbyte-worker-job=<read_id>`, so timeout and cancel kill only that read's container.
- Logging: one line when a read starts and one when it ends (status, counts, batches, duration); no credentials or record data.

## Sync modes

- Incremental needs a cursor column (for example `updated_at`). Airbyte compares the cursor with `>=`, so each run returns the boundary row again; it is counted as unchanged.
- Tables without a cursor are read in full; the hash finds the changed rows.

## Known limits

- One tested source: Postgres through `source-postgres`.
- A read takes 11-25 s before the first record because the Java connector starts. Records then arrive quickly (50,000 rows in about 24 s in total on a laptop).
- Timestamps from `timestamptz` columns come back in UTC (same instant, original offset not kept).
- Rejected records are counted and logged, not stored.
- Recovery beyond "state is saved only after commit" is not implemented yet.
