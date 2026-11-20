# Design notes

## Goal

Load records from Airbyte sources into InterSystems IRIS, with all setup and operation in an IRIS production. No Airbyte server, no Airbyte web UI.

## Components

| Component | Runs in | Role |
|---|---|---|
| Worker | Own container, Python 3.11 | FastAPI service around PyAirbyte. Runs the Airbyte connector, converts values, returns records in batches and the new source state |
| Airbyte connector | Docker container started by the worker | `airbyte/source-postgres`, the same image the Airbyte platform uses |
| `Airbyte.Demo.Service.Extract` | IRIS | Business Service: settings, credentials, run lock, async read with polling, sends batches in order as they arrive, retries, saves state |
| `Airbyte.Demo.Operation.Store` | IRIS | Business Operation: insert-or-update, one transaction per batch |
| `Airbyte.Demo.Data.Record`, `Run`, `RunStream`, `ConnectionStatus`, `Rejected` | IRIS | Stored records, run counters (per run and per stream), where each connection stands, and records with a bad key |

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
| POST | `/reads/{id}/ack` | The caller has stored every batch and the state: delete the read's batch files (`status.json`, `progress.json` and `state.json` stay). Only for completed reads (`409 not_completed` otherwise); repeating it is harmless |
| DELETE | `/reads/{id}` | Stop a running read (kills its process and its connector container); status becomes `cancelled`, no state. `409 not_running` for a finished read |

Record format:

```json
{"stream": "customers", "key": ["1"], "data": {"id": 1, "name": "Alice Smith", "balance": "120.50", "updated_at": "2026-11-01T10:00:00+00:00", "email": "alice@example.com"}}
```

## Connector settings

IRIS does not know which connector it talks to. The Extract service has `ConnectorName`, `ConnectorVersion`, `ConnectorConfig` (the connector's own settings as JSON, without secrets) and `CredentialFields` (`config.path=CredentialsProperty` pairs, dotted paths for nested fields). At run time `BuildConfig` puts the values from the IRIS Credentials entry into the config at those paths. The worker validates the config against the connector's spec before check, discover and read, and hides the spec's secret fields in error messages. `Util.Spec()`, `Util.Check()` and `Util.Discover()` show the spec, validate the connection and list the streams from the IRIS terminal.

## Runs, ordering and retries (IRIS side)

- **Pipelined.** Extract starts the read with `wait=false` and a `connection_id`, polls `GET /reads/{id}` every `PollInterval` seconds (default 2) and, as soon as `batches_ready` grows, fetches each new batch and sends it to Store while the worker is still reading. The state is fetched and saved only when the read is `completed` and every batch is stored. A failed or cancelled read, or a batch that did not store, stops the run without saving the state.
- **Ordering rule.** Within one connection, batches are stored strictly in order, one at a time (a later batch can hold a newer version of a record an earlier batch already wrote). Parallelism is across connections: Extract items with different `ConnectionId` run at the same time and their records stay separate (the identity includes the connection). Store has two jobs for that reason. One run per `ConnectionId` at a time.
- **Time limits.** `ReadTimeout` (default 600 s) limits the whole run, retries included. Each HTTP call to the worker uses `HttpTimeout` (default 30 s).
- **Error handling.** Transient errors (worker unreachable, HTTP 5xx, categories `connection`, `timeout`, `internal`, `connector`) repeat the whole read from the last saved state, up to `MaxRetries` (default 3) times, waiting `BackoffSeconds * 2^(n-1)` (5, 10, 20 s). This is safe because the state moves only after commit and the upsert never duplicates. Configuration problems (`auth`, `config`, `validation`, `not_found`) stop the run at once with status `ConfigError` and a message telling the operator what to fix. A busy worker (409) skips the run. Anything else (cancelled, a batch that failed to store, an unexpected IRIS error) stops the run as `Failed` without retry. The Run table shows `RetryCount` and `ErrorCategory`.
- **Counters across retries.** A retry reads again from the old state, so records stored by the failed attempt count as unchanged in the retry. The Run row adds up what every attempt stored.
- **Credentials.** `CredentialFields` entries `path=Username` or `path=Password` use the entry from the `Credentials` setting. `path=Entry.Property` uses another credentials entry, so a connector with more than two secrets can use several entries.

## Run history and rejected records

- **History.** The Store operation writes the counts per run (`Run`) and per stream (`RunStream`) in the same transaction as each batch. `Run` also has `DurationSeconds`, `RetryCount` and `ErrorCategory`. At the end of every run the service updates one row per connection in `ConnectionStatus` (last run and status, last successful run and time, category and message of the last error; a skipped run does not overwrite the last error) and writes one Event Log line with connection, status, counts, duration and retries (no data). `Util.Runs()`, `Util.RunDetail(runId)` and `Util.Connections()` show it.
- **Rejected records.** A record whose key is missing, empty or null is not stored. Store writes it to `Rejected` (run, batch, connection, stream, reason, the record data as JSON, status New or Ignored) in the same transaction as the batch. The reason never contains a value; the payload is only in that table, never in logs. `Util.Rejects()` lists them (`Util.Rejects(1)` with payload) and `Util.IgnoreRejected(id)` marks one as Ignored.
- **How to fix a rejected record.** Fix it at the source (fill in the missing key value), or change which columns form the key with the `KeyFields` setting (`stream=col1+col2,...`, passed to the worker as `key_fields`, for streams without a usable primary key). The next run reads the record again and stores it. A record that is still wrong is rejected again and gets a new row for that run; rows are kept until you delete them.

## Data minimisation

- **Batch messages.** Store has the setting `ClearBatchBodies` (default 1). After a batch is committed, the record data in the stored `Batch` message is emptied, so message storage keeps the headers and counts (Visual Trace still shows every message) but no second copy of the data. Set it to 0 to keep the data in the messages for debugging.
- **Purge on a schedule.** Message headers and the Event Log still grow with every run. Schedule the standard purge task (`Ens.Util.Tasks.Purge`, System Management Portal > Task Manager) with `NumberOfDaysToKeep` 7, `KeepIntegrity` on and `BodiesToo` on, daily. Keep more days if you need a longer history.
- **Worker.** After the state is saved, Extract calls `POST /reads/{id}/ack` and the worker deletes the batch files of that read. Each job's temp files (config with credentials, catalog, state) live in a folder of their own that is removed when the job ends, also after a timeout or a cancel; a startup sweep removes anything an earlier crash left. The connector's own log files do not contain the config or credentials (checked by searching `/data` and `/tmp` for the Postgres password after reads).

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
- A read takes 11-25 s before the first record because the Java connector starts. Records then arrive quickly (50,000 rows in about 24 s in the worker; about 25-30 s through IRIS, where storing is the slower part).
- A wrong host or a stopped source is reported by the connector as a generic connector error, so it is retried like a temporary error before the run fails.
- Timestamps from `timestamptz` columns come back in UTC (same instant, original offset not kept).
- Recovery beyond "state is saved only after commit" is not implemented yet.
