# iris-airbyte-adapter

Load data from Airbyte sources into InterSystems IRIS. You set it up and run it entirely from an IRIS production: no Airbyte server, no Airbyte web UI.

> **Status: prototype, v0.1.0.** It reads a Postgres source through the Airbyte `source-postgres` connector and stores the records in IRIS with insert-or-update, no duplicates and a saved source state. The IRIS storage part (package `Airbyte.Demo`) is prototype code. The extraction worker is independent of any IRIS framework. See [Known limits](#known-limits).

![Architecture](demo/architecture.png)

## How it works

1. An IRIS Business Service (`Airbyte.Demo.Service.Extract`) asks the worker to read, passing the last committed source state.
2. The worker (Python, FastAPI + [PyAirbyte](https://github.com/airbytehq/PyAirbyte)) runs the real Airbyte `source-postgres` connector in its own Docker container and returns the records in batches, plus the new state.
3. A Business Operation (`Airbyte.Demo.Operation.Store`) writes each batch in one transaction: insert new keys, update changed records, skip unchanged ones (SHA-256 hash), and put records with a missing key into a table of rejected records.
4. The service saves the new state only after every batch is committed.

The identity of a record is connection + stream + key, so the same key in two tables never collides. Reruns create no duplicates, and records that disappear from the source are never deleted in IRIS.

## What is in the repo

| Path | What |
|---|---|
| `worker/` | Extraction worker (FastAPI + PyAirbyte) and its pytest tests |
| `src/cls/Airbyte/Demo/` | IRIS production: service, operation, messages, tables, helpers |
| `src/cls/Airbyte/Test/` | IRIS unit tests |
| `iris/` | IRIS image setup |
| `demo/` | Seed data and live-change scenarios |
| `bench/` | Benchmark scripts, raw results and [the report](bench/REPORT.md) |
| `docs/design.md` | Design notes and the worker API |
| `docker-compose.yml` | IRIS, Postgres (demo source), worker |

## Quick start

**Prerequisites:** Docker Desktop (or Docker Engine) with Compose v2, about 6 GB of memory for Docker, and free ports 52773, 1972, 8000 and 5433.

```bash
git clone https://github.com/mwaseem75/iris-airbyte-adapter.git
cd iris-airbyte-adapter
cp .env.example .env                          # then set POSTGRES_PASSWORD in .env
docker pull airbyte/source-postgres:3.8.11    # the connector image (otherwise the first run downloads it)
docker compose up -d --build --wait           # IRIS, Postgres with demo data, worker
```

First run, in the IRIS container (`docker exec -it iris iris session IRIS -U USER`):

```objectscript
do ##class(Airbyte.Demo.Util).Status()              // production running, credentials present
do ##class(Airbyte.Demo.Service.Extract).RunNow()   // first run: 13 read, 13 inserted
do ##class(Airbyte.Demo.Util).Records()             // rows per stream
do ##class(Airbyte.Demo.Service.Extract).RunNow()   // rerun: 7 read, 7 unchanged, no duplicates
```

Change the source and run again:

```bash
docker exec postgres psql -U demo -d demo -f /scenarios/01_update_customer.sql
docker exec postgres psql -U demo -d demo -f /scenarios/02_insert_customer.sql
# RunNow again: 1 inserted, 1 updated
docker exec postgres psql -U demo -d demo -f /scenarios/03_update_order_line.sql
# RunNow again: order_lines 1 updated, 4 unchanged
```

To reset the source use `/scenarios/00_reset_source.sql`; to reset the IRIS side use `do ##class(Airbyte.Demo.Util).Reset()`.

Management Portal: http://localhost:52773/csp/sys/UtilHome.csp (local demo login `_SYSTEM` / `SYS`). The Message Viewer shows each run as Extract → Store messages. A step-by-step walkthrough with expected results is in [DEMO.md](DEMO.md).

## Configuration

### Extract settings (Business Service `Extract`)

Set them on the production page of the Management Portal.

| Setting | Default | Meaning |
|---|---|---|
| `ConnectorName` | `source-postgres` | Airbyte connector to run. The worker only runs connectors in `ALLOWED_CONNECTORS` |
| `ConnectorVersion` | `3.8.11` | Version (image tag) of the connector |
| `ConnectorConfig` | demo Postgres | The connector's settings as JSON, **without secrets**. Field names are the connector's own (see `Util.Spec()`) |
| `Credentials` | `Postgres` | Name of the IRIS Credentials entry that holds the secrets |
| `CredentialFields` | `username=Username,password=Password` | Which config fields take their value from a credentials entry: `path=Property` (default entry), `path=Entry.Property` (another entry). Nested fields use dots |
| `Streams` | `customers,order_lines,suppliers` | Comma separated stream (table) names |
| `CursorFields` | `customers=updated_at,suppliers=updated_at` | `stream=column` pairs. A stream without a cursor is read in full |
| `KeyFields` | empty | `stream=col1+col2` pairs: the columns that form the record key, for streams without a usable primary key. Records where one of them is missing are rejected |
| `Mode` | `incremental` | `incremental` or `full_refresh` |
| `BatchSize` | `500` | Records per batch (1,000 to 5,000 are good for large loads) |
| `PollInterval` | `2` | Seconds between status checks while the worker reads |
| `ReadTimeout` | `600` | Time limit in seconds for the whole run, retries included |
| `HttpTimeout` | `30` | Time limit in seconds for each call to the worker |
| `MaxRetries` | `3` | Repeats after a temporary error (worker unreachable, connection, timeout) |
| `BackoffSeconds` | `5` | Wait before retry n is `BackoffSeconds × 2^(n-1)`: 5, 10, 20 s |
| `PollingEnabled` | `0` | `1` lets the call interval start runs; `0` means only `RunNow()` starts a run |
| `WorkerURL` | `http://worker:8000` | Where IRIS reaches the worker |
| `ConnectionId` | `demo` | Name of this connection. It keys the saved state and the stored records |

### Store settings (Business Operation `Store`)

| Setting | Default | Meaning |
|---|---|---|
| `ClearBatchBodies` | `1` | After a batch is committed, empty the record data in the stored message (headers stay, so Visual Trace still works) |

### Worker environment variables (`docker-compose.yml`, `.env`)

| Variable | Default | Meaning |
|---|---|---|
| `READ_TIMEOUT_SECONDS` | `120` | Whole-read time limit in the worker. Use 1800 or more for reads of about a million rows |
| `MAX_CONCURRENT_READS` | `2` | Operations (reads, checks, discovery) the worker runs at the same time. More are answered `409 busy` |
| `ALLOWED_CONNECTORS` | `source-postgres` | Comma separated connectors the worker may run |
| `KEEP_READS` | `10` | How many read folders the worker keeps |
| `CATALOG_CACHE_TTL_SECONDS` | `3600` | How long the worker reuses a connector's discovered stream list. `0` turns it off |

## Using another connector

Only `source-postgres` is tested (see below). The IRIS side does not depend on the connector, so you can try another one that the worker is allowed to run:

1. Add it to `ALLOWED_CONNECTORS` and restart the worker.
2. Set `ConnectorName` and `ConnectorVersion` on the Extract item.
3. Run `do ##class(Airbyte.Demo.Util).Spec()`: it lists the connector's settings and marks the required and secret fields.
4. Put the non-secret settings into `ConnectorConfig` as JSON. Put each secret in an IRIS Credentials entry and map it with `CredentialFields` (for example `credentials.client_secret=MyApp.Password`). A value for a secret field inside `ConnectorConfig` is reported as a warning (the field name only).
5. `do ##class(Airbyte.Demo.Util).Check()` validates the connection, and `Discover()` lists streams, keys, cursors and sync modes.
6. Set `Streams`, `CursorFields` and `KeyFields`, then `RunNow()`.

## Operations

All helpers are in `Airbyte.Demo.Util` (call them from an IRIS terminal in `USER`).

| Call | Shows |
|---|---|
| `Status()` | Production state and credentials |
| `Spec()`, `Check()`, `Discover()` | The connector's settings, a connection check, the streams |
| `Records()` | Rows per stream |
| `Runs()` | Run history: counts, duration, store time, retries, error category |
| `RunDetail(runId)` | Counts per stream for one run |
| `Connections()` | Last run and last successful run per connection, and the last error |
| `Rejects()` / `Rejects(1)` | Records rejected because of a bad key (with the record data) |
| `IgnoreRejected(id)` | Marks one rejected record as ignored |
| `State(connectionId)` | The saved source state |
| `ResetConnection(connectionId)` | Clears one connection (records, runs, rejects, status, state) and leaves the others |
| `Reset()` | Clears everything on the IRIS side |

**Purge.** Message headers and the Event Log grow with every run. Schedule the standard purge task (`Ens.Util.Tasks.Purge`, System Management Portal > Task Manager) daily with `NumberOfDaysToKeep` 7, `KeepIntegrity` on and `BodiesToo` on.

**Rejected records.** A record with a missing or empty key is not stored. It goes to the table of rejected records with the reason. Fix the value at the source, or set `KeyFields`, and run again.

## Security

- **Credentials** live in IRIS Credentials entries only. They are sent to the worker with each request and are not stored there.
- **Nothing secret in logs.** Credentials and record data are not written to the IRIS Event Log or the worker log. Error messages from the connector are scrubbed of secret values (the connector's own list of secret fields is used).
- **Config is validated** against the connector's spec before every check, discovery and read. A wrong config is reported with field names and reasons, never values.
- **Data minimisation.** Batch message bodies are emptied after the commit (`ClearBatchBodies`); the worker deletes each batch file as soon as IRIS has committed it, and the batch files of a failed or cancelled read when it ends. Each job's config, catalog and state files are in a per-job temp folder that is removed when the job ends (also after a timeout or cancel), and the worker removes leftovers at start. The cache of discovered streams holds no config: its key is a salted hash.
- **The worker needs the Docker socket** (it starts the connector as a container). Run it on a trusted host, do not expose port 8000 publicly, and keep `ALLOWED_CONNECTORS` as short as possible.
- The demo uses local demo credentials (`_SYSTEM` / `SYS`, the Postgres password from `.env`). Change them for anything that is not a local demo.

## Troubleshooting

| Symptom | What to do |
|---|---|
| `Status()` shows `STOPPED` | `do ##class(Airbyte.Demo.Production).Setup()`, or press Start on the Production page |
| After loading classes by hand, the old behaviour continues | Restart the production: `do ##class(Ens.Director).RestartProduction()`. Running jobs keep the old code (a container restart does this too) |
| A run says `ConfigError` | The message says what to fix, for example `authentication failed: check the credentials entry 'Postgres'`. Nothing is retried. Fix it and run again |
| `temporary error ... retry 1 of 3` | The worker or the source was not reachable. The run repeats from the saved state after 5, 10 and 20 s |
| A run says `timeout` | The run took longer than `ReadTimeout`. Raise it (and `READ_TIMEOUT_SECONDS` in the worker) for large reads. The state was not saved; run again |
| `Skipped ... worker is busy` or `another run is active` | A read for this connection is still running, or the worker is at `MAX_CONCURRENT_READS`. Wait and try again |
| The connector cannot reach Postgres | Check `docker compose ps`. The connector uses `host.docker.internal:5433`; change `POSTGRES_HOST_PORT` in `.env` and the `port` in `ConnectorConfig` if 5433 is taken |
| The first run is slow | The first run after a start takes longer (about 45 s): the connector image is started and the spec is fetched |
| Counts differ from this page | A step was already run. Reset (`Util.Reset()` and `/scenarios/00_reset_source.sql`) and start again |

## Compatibility

Exactly what was tested. Other versions and platforms may work but are not tested.

| Component | Version |
|---|---|
| InterSystems IRIS Community | 2026.1 (build 234U, `latest-em`, Ubuntu container) |
| Host | Windows 11 (build 22631) with Docker Desktop, WSL 2 engine |
| Docker | Engine 28.1.1, Compose 2.35.1 |
| Worker | Python 3.11.18, FastAPI 0.115.6, uvicorn 0.35.0 |
| PyAirbyte | 0.71.3 (airbyte-cdk 7.49.0) |
| Connector | `airbyte/source-postgres:3.8.11` |
| Source | Postgres 16 in Docker (16.16) |

Not tested yet: Linux hosts, other connectors, Postgres on AWS RDS.

## Benchmarks

On a laptop with local Docker, 100,000 new rows load in about a minute and one million rows in about 7.5 minutes; storing in IRIS is the slower part, and every run has a fixed cost of about 18 s. Details, the method and the raw results: [bench/REPORT.md](bench/REPORT.md).

## Known limits

- One tested source: Postgres via `source-postgres`. The worker only runs connectors listed in `ALLOWED_CONNECTORS`.
- Each run takes at least about 18 s because the read process and the Java connector start.
- Incremental reads need a cursor column (for example `updated_at`). Airbyte returns the boundary row again on each run, and it is counted as unchanged.
- A stopped or wrong source host is reported by the connector as a generic error, so the run is retried before it fails.
- The worker reads faster than IRIS stores, so its batch files wait for IRIS (about 400 MB for one million rows).
- Recovery beyond "the state is saved only after the commit" is not implemented: a failed run starts again from the last saved state.
- Prototype: demo credentials, one IRIS production, no schema-change handling.

## More

[CHANGELOG.md](CHANGELOG.md) · [DEMO.md](DEMO.md) · [docs/design.md](docs/design.md) · [bench/REPORT.md](bench/REPORT.md)

## License

MIT (this repository's code), see [LICENSE](LICENSE). Third-party: PyAirbyte and the Airbyte connectors are licensed by Airbyte, Inc. under the Elastic License 2.0 (ELv2) or MIT, depending on the component; check the license of each connector you use.
