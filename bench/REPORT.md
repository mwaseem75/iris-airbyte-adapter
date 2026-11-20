# Benchmark report (local Docker)

First measurements of the adapter on one machine: Postgres, the PyAirbyte worker, the source connector and IRIS all run in Docker next to each other. A run on AWS RDS with the same scripts will follow. Everything here can be repeated with the scripts in `bench/` (see "How to run").

## Findings

- **IRIS is the slow part on large loads.** One million rows took 12 minutes (722 s). The worker had read all of them after 130 s (about 7,700 records/s); IRIS needed 588 s to store them (about 1,700 records/s). The worker works ahead of IRIS, so total time follows the store time.
- **Every run has a fixed cost of about 20 to 25 seconds**, however small the read is. A rerun that reads one record took 26 s; 10,000 records took 19 s; a mixed run with 10,000 records took 30 to 36 s. Starting the read process and the connector container and discovering the schema is not broken down further here. Polling more often than once a minute does not make sense.
- **Throughput on 100,000 rows is 1,100 to 2,000 records/s and varies a lot from run to run.** The same initial load took 51 s and 74 s, and the setup loads of other scenarios took 88 to 105 s. The machine is a laptop whose CPU is shared by all four components, so single numbers have a wide margin.
- **Updating and re-reading is cheap compared with inserting.** A full re-read of 100,000 unchanged records was stored in 17 s (about 5,800 records/s, compared by hash) against 23 to 56 s for the first load.
- **The batch size makes no clear difference between 500 and 5,000**, inside that variation (83 s, 51 to 105 s, 88 s and 74 s for 500, 1,000, 2,000 and 5,000).
- **Recovery works.** A load of 100,000 rows was cancelled after 50 batches had been read (IRIS had stored 20,000 rows by then). The rerun stored 80,000 new rows and found 20,000 unchanged. IRIS held exactly 100,000 rows afterwards, no duplicates.
- **Ten mixed runs in a row stayed flat** (30 to 36 s each, store time 2.7 to 4.8 s, no growth). A run with an incremental cursor reads the rows changed since the last run plus the rows that share the newest cursor value, so each of runs 2 to 10 read 5,000 changed and 5,000 unchanged records.
- **Memory:** for one million rows the connector container peaked at 1.8 GB, IRIS at 1.1 GB, Postgres at 0.4 GB and the worker at 0.4 GB. The worker keeps every batch file of a read until IRIS acknowledges the read, so its read folder grew to 441 MB for one million rows (about 460 bytes of JSON per record).
- **CPU:** the connector alone used 265 to 475 % of a core (100 % = one core), the worker up to 200 % and IRIS 110 to 140 %. On a machine with few cores they compete with each other and with the store.

## Environment

The full list is in `bench/results/environment.json`.

| | |
|---|---|
| Machine | Intel Core i7-8665U (4 cores, 8 threads), 15.8 GB RAM, Windows 11 |
| Docker Desktop (WSL 2) | 8 CPUs and 4.8 GB memory available to containers |
| IRIS | 2026.1 (build 234U), Community, automatic global buffers (default) |
| Worker | Python 3.11, PyAirbyte 0.71.3, airbyte-cdk 7.49.0 |
| Connector | `airbyte/source-postgres:3.8.11` |
| Source | `postgres:16` in Docker, table `bench_items` |
| Worker limits | `READ_TIMEOUT_SECONDS=1800`, `MAX_CONCURRENT_READS=2` |
| Adapter settings | `BatchSize` 1000 (unless the scenario says otherwise), `PollInterval` 2 s, incremental on `updated_at` |

The test table `bench_items` has an id, name, email, amount, status, a 100-character note, a small nested JSON object and `updated_at`. A record is about 460 bytes of JSON in a batch (the batch also carries the key).

## Results per scenario

"Total" is the run time recorded by IRIS (`Run.DurationSeconds`). "Worker" is the time the worker spent on the read, from the start request until the read finished, including starting the connector. "Store" is the time the Store operation spent writing batches in IRIS (`Run.StoreSeconds`). Worker and store time overlap, because IRIS stores batches while the worker is still reading, so they do not add up to the total. MB/s counts the batch JSON the worker produced. The scenario `initial_100k` was run twice; both rows are listed. In `recovery_100k_interrupted` the worker has no duration because the read was cancelled.

| Scenario | Rows in source | Batch | Read | Inserted | Updated | Unchanged | Total s | Worker s | Store s | Records/s | MB/s | Status |
|---|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|---|
| initial_10k | 10,000 | 1000 | 10,000 | 10,000 | 0 | 0 | 19.07 | 15.29 | 2.53 | 524 | 0.24 | Completed |
| initial_100k | 100,000 | 1000 | 100,000 | 100,000 | 0 | 0 | 50.94 | 25.71 | 23.5 | 1,963 | 0.9 | Completed |
| mixed_100k | 105,000 | 1000 | 15,000 | 5,000 | 10,000 | 0 | 34.1 | 24.42 | 7.07 | 439 | 0.2 | Completed |
| unchanged_100k | 100,000 | 1000 | 1 | 0 | 0 | 1 | 26.16 | 25.77 | 0.0 | 0 | 0.0 | Completed |
| batch_500 | 100,000 | 500 | 100,000 | 100,000 | 0 | 0 | 83.16 | 31.44 | 50.08 | 1,202 | 0.55 | Completed |
| batch_2000 | 100,000 | 2000 | 100,000 | 100,000 | 0 | 0 | 87.83 | 36.09 | 51.29 | 1,138 | 0.52 | Completed |
| batch_5000 | 100,000 | 5000 | 100,000 | 100,000 | 0 | 0 | 73.98 | 36.89 | 36.49 | 1,351 | 0.62 | Completed |
| initial_1m | 1,000,000 | 1000 | 1,000,000 | 1,000,000 | 0 | 0 | 722.07 | 129.62 | 588.18 | 1,384 | 0.64 | Completed |
| recovery_100k_interrupted | 100,000 | 1000 | 20,000 | 20,000 | 0 | 0 | 53.22 | 0.0 | 11.48 | 375 | 0.51 | Failed (read cancelled on purpose) |
| recovery_100k_rerun | 100,000 | 1000 | 100,000 | 80,000 | 0 | 20,000 | 89.81 | 38.41 | 49.89 | 1,113 | 0.51 | Completed |
| unchanged_100k_full_refresh | 100,000 | 1000 | 100,000 | 0 | 0 | 100,000 | 49.39 | 29.36 | 17.18 | 2,024 | 0.92 | Completed |
| initial_100k | 100,000 | 1000 | 100,000 | 100,000 | 0 | 0 | 74.4 | 32.65 | 40.92 | 1,344 | 0.61 | Completed |

Ten mixed runs in a row (`sustained_100k`, 5 % of the rows updated before each run, 100,000 rows in the source):

| Run | Read | Updated | Unchanged | Total s | Worker s | Store s |
|--:|--:|--:|--:|--:|--:|--:|
| 01 | 5,001 | 5,000 | 1 | 33.17 | 28.28 | 2.7 |
| 02 | 10,000 | 5,000 | 5,000 | 33.37 | 28.75 | 4.5 |
| 03 | 10,000 | 5,000 | 5,000 | 30.82 | 25.19 | 3.79 |
| 04 | 10,000 | 5,000 | 5,000 | 35.6 | 29.68 | 4.56 |
| 05 | 10,000 | 5,000 | 5,000 | 32.53 | 26.61 | 3.69 |
| 06 | 10,000 | 5,000 | 5,000 | 31.35 | 26.36 | 4.3 |
| 07 | 10,000 | 5,000 | 5,000 | 30.57 | 25.28 | 3.89 |
| 08 | 10,000 | 5,000 | 5,000 | 33.76 | 29.01 | 4.78 |
| 09 | 10,000 | 5,000 | 5,000 | 32.92 | 27.7 | 4.07 |
| 10 | 10,000 | 5,000 | 5,000 | 30.4 | 24.72 | 3.69 |

## Resource peaks

Highest values seen while the run was active, sampled with `docker stats` (one pass takes about a second). CPU is in percent of one core. "Staging" is the size of the worker's read folder. Memory of IRIS is the whole container and includes cache from earlier runs, so it only grows within a session.

| Scenario | worker MB | IRIS MB | Postgres MB | connector MB | worker CPU % | IRIS CPU % | Postgres CPU % | connector CPU % | Staging MB |
|---|--:|--:|--:|--:|--:|--:|--:|--:|--:|
| initial_10k | 253 | 418 | 33 | 365 | 198 | 72 | 9 | 344 | 4.6 |
| initial_100k | 254 | 477 | 66 | 422 | 135 | 113 | 39 | 344 | 44.0 |
| mixed_100k | 274 | 468 | 78 | 270 | 148 | 112 | 33 | 350 | 32.0 |
| unchanged_100k | 281 | 436 | 75 | 243 | 196 | 13 | 14 | 394 | 25.6 |
| batch_500 | 279 | 426 | 83 | 390 | 143 | 121 | 29 | 268 | 44.0 |
| batch_2000 | 286 | 436 | 83 | 372 | 141 | 119 | 47 | 307 | 43.9 |
| batch_5000 | 300 | 459 | 83 | 405 | 185 | 116 | 37 | 275 | 43.8 |
| initial_1m | 417 | 1,064 | 396 | 1,848 | 167 | 139 | 48 | 265 | 441.2 |
| recovery_100k_interrupted | 315 | 1,142 | 207 | 415 | 158 | 110 | 51 | 475 | 26.0 |
| recovery_100k_rerun | 353 | 1,142 | 208 | 427 | 200 | 126 | 28 | 419 | 69.7 |
| unchanged_100k_full_refresh | 373 | 1,143 | 209 | 432 | 144 | 138 | 24 | 390 | 69.7 |
| initial_100k | 373 | 1,143 | 211 | 437 | 196 | 111 | 51 | 367 | 69.7 |
| sustained_100k_01 | 282 | 427 | 78 | 325 | 190 | 110 | 11 | 271 | 25.6 |
| sustained_100k_10 | 275 | 428 | 78 | 322 | 113 | 104 | 11 | 311 | 4.7 |

## Where the time goes

- **Large initial load:** the store step. At one million rows it is 588 s of 722 s. Worker and Postgres are not the limit; both are idle for most of the run.
- **Small incremental run:** the fixed cost of starting a read (20 to 25 s). The records themselves take only 3 to 5 seconds to store.
- **Unchanged data:** reading is the main cost; storing compares hashes and writes nothing.

## Recommended settings

- **`BatchSize`:** 1,000 to 5,000. The tests show no difference worth tuning for; 1,000 keeps each transaction and each HTTP call small.
- **`CallInterval` (how often the service polls):** 60 seconds or more for incremental loads, because a run never takes less than about 20 s.
- **Worker `READ_TIMEOUT_SECONDS`:** 1800 or more for reads of a million rows or more. The worker part of one million rows took 130 s here, but a slower source or network will take longer. The default of 120 is only enough for small reads.
- **`ReadTimeout` of the Extract service:** 3600 for large loads (one million rows took 12 minutes here).
- **Docker resources:** at least 4 CPUs and 6 GB of memory for loads of about a million rows (the containers together peaked at about 3.7 GB of the 4.8 GB that were available); 8 GB gives room.
- **Purge:** schedule the standard message purge. A large load creates one message per batch.

## Limits of this test

- One machine, one run per scenario (`initial_100k` twice). The same scenario differed by up to a factor of two between runs, so treat the figures as orders of magnitude, not exact values.
- Everything shares one laptop CPU. On separate hosts the store and the extraction would not compete.
- No network latency: Postgres runs next to the worker. AWS RDS results will follow with the same scripts.
- The memory and CPU figures are samples about a second apart; short peaks can be missed. CPU above 100 % means more than one core.
- The staging figure also counts files left by earlier cancelled reads (the worker keeps the last 10 read folders).
- Only `source-postgres` with the table described above was measured: no wide rows, no large text or binary columns.
- The scenario `unchanged_100k` uses the incremental cursor, so it reads a single record. `unchanged_100k_full_refresh` reads all 100,000.

## How to run

You need Python 3 on the machine that runs Docker, the stack running (`docker compose up -d`) and `.env` set. Set `READ_TIMEOUT_SECONDS=1800` in `.env` for the large scenarios and restart the worker.

```
bench/run.sh list                # scenario names
bench/run.sh initial_100k        # one scenario (PowerShell: bench\run.ps1 initial_100k)
bench/run.sh environment         # rewrite results/environment.json
```

Each run adds a row to `bench/results/results.csv`; the connection `bench` and the table `bench_items` are the only things changed. Scenarios: `initial_10k`, `initial_100k`, `initial_1m`, `mixed_100k`, `unchanged_100k`, `unchanged_100k_full`, `recovery_100k`, `sustained_100k`, `batch_500`, `batch_2000`, `batch_5000`. The SQL for the test table is in `bench/sql/`. `Airbyte.Demo.Util.ResetConnection("bench")` clears the IRIS side of the benchmark connection; drop the table with `bench/sql/drop_items.sql`.
