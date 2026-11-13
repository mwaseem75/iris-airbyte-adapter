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
| POST | `/check` | Validate config and connection |
| POST | `/discover` | Streams, schemas, primary keys, sync modes |
| POST | `/reads` | Read selected streams; body carries config, streams, mode, batch size and the input state |
| GET | `/reads/{id}` | Status and counts |
| GET | `/reads/{id}/batches/{n}` | One batch of records |
| GET | `/reads/{id}/state` | Source state after the read |

Record format:

```json
{"stream": "customers", "key": ["1"], "data": {"id": 1, "name": "Alice Smith", "balance": "120.50", "updated_at": "2026-11-01T10:00:00+00:00", "email": "alice@example.com"}}
```

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
- A read runs in a child process with a timeout (default 120 s). On timeout the process and the connector container are stopped and the API returns a `timeout` error.
- One connector operation at a time; a second request gets `409 busy`.

## Sync modes

- Incremental needs a cursor column (for example `updated_at`). Airbyte compares the cursor with `>=`, so each run returns the boundary row again; it is counted as unchanged.
- Tables without a cursor are read in full; the hash finds the changed rows.

## Known limits

- One tested source: Postgres through `source-postgres`.
- Reads are synchronous; each read takes about 25 s because the Java connector starts.
- Timestamps from `timestamptz` columns come back in UTC (same instant, original offset not kept).
- Rejected records are counted and logged, not stored.
- Recovery beyond "state is saved only after commit" is not implemented yet.
