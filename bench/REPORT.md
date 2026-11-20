# Benchmark report (local Docker)

First measurements of the adapter on one machine: Postgres, the PyAirbyte worker, the source connector and IRIS all run in Docker next to each other. A run on AWS RDS with the same scripts will follow. Everything here can be repeated with the scripts in `bench/` (see "How to run").

## Findings

The first measurements (section "Results per scenario") showed that storing in IRIS was the slow part and that every run had a fixed cost of 20 to 25 s. This version of the report adds what was found and changed after that.

- **Where the store time went.** For 100,000 new records, IRIS spent 49 % of the store time writing the records, 24 % computing the hash, 15 % looking up whether the record exists, and only 0.1 % on the commit. Counting, parsing and the transaction are not the problem.
- **Two changes in Store made it a quarter to a third faster.** The worker now sends the hash of each record, so IRIS only compares it (the hash step went from 11.3 s to 0.3 s), and the lookup goes through the index without opening the record (7.0 s to 3.5 s). The store time for 100,000 new records fell from 47.9 s to 31.7 s in the profile run, and from a median of 40.9 s to 31.9 s in the repeated runs.
- **One million rows now take 7.6 minutes (455 s), down from 12 minutes (722 s).** Storing is still the longest part (317 s of 455 s). Writing the records is now 70 % of the store time; a plain SQL `INSERT` costs the same per record (about 220 microseconds), so this part was not changed.
- **The fixed cost per run went from 24 to 18 s** (median of the unchanged-data scenario). The read started the connector twice: once to discover the streams and once to read. The discovery is now cached, which saves 6 to 7 s per run. Starting the read process and importing PyAirbyte takes another 6 to 9 s and is the largest part left.
- **Batch files are deleted as soon as IRIS has committed them.** The worker folder still grew to 394 MB for one million rows, because the worker reads about twice as fast as IRIS stores and the files wait for IRIS. Only slowing the worker down would shrink that.
- **Recovery, resource use and the rest are unchanged** from the first measurements below.

## Before and after

Median of three runs (minimum to maximum) for the first four scenarios, one run for one million rows. "Before" is the code of the first measurements; its rows are kept in `bench/results/results.csv` with `version=before`, the new ones are `version=after`. The "before" values of `initial_100k` are the two first runs plus a third one taken just before the change.

**Total seconds** - median (min-max)

| Scenario | Before | After | Change of the median |
|---|--:|--:|--:|
| initial_10k | 32.5 (19.1-35.0), n=3 | 24.6 (22.2-28.8), n=3 | -24 % |
| initial_100k | 74.4 (50.9-97.0), n=3 | 63.7 (61.5-64.3), n=3 | -14 % |
| mixed_100k | 35.0 (34.1-35.0), n=3 | 24.8 (24.3-25.0), n=3 | -29 % |
| unchanged_100k | 26.2 (24.1-30.2), n=3 | 18.2 (18.2-24.1), n=3 | -31 % |
| initial_1m | 722.1, n=1 | 454.8, n=1 | -37 % |

**Store seconds** - median (min-max)

| Scenario | Before | After | Change of the median |
|---|--:|--:|--:|
| initial_10k | 5.4 (2.5-5.8), n=3 | 3.6 (3.4-3.7), n=3 | -33 % |
| initial_100k | 40.9 (23.5-57.1), n=3 | 31.9 (31.1-33.1), n=3 | -22 % |
| mixed_100k | 7.6 (7.1-8.0), n=3 | 5.5 (5.1-5.7), n=3 | -27 % |
| unchanged_100k | 0.0 (0.0-0.0), n=3 | 0.0 (0.0-0.0), n=3 | - |
| initial_1m | 588.2, n=1 | 316.6, n=1 | -46 % |

Throughput of the 100,000-row load: 1,030 to 1,960 records/s before, 1,550 to 1,625 after. One million rows: 1,385 records/s before, 2,199 after.

The numbers still vary between runs on this laptop: the same scenario ranged from 51 to 97 s before. The "after" runs were taken one after the other in one session, so their spread (62 to 64 s) is probably narrower than it would be over a day. Read the change as "about a quarter to a third faster", not as exact percentages.

## Where the store time goes

Seconds spent in the Store operation while loading 100,000 new records (`initial_100k`, measured with the debug timing switch described in `docs/design.md`; the timing itself adds a little).

| Phase | Before s | Before % | After s | After % |
|---|--:|--:|--:|--:|
| Parse the batch JSON | 0.85 | 1.8 | 0.80 | 2.5 |
| Serialize the record data | 1.35 | 2.8 | 1.46 | 4.6 |
| Hash | 11.31 | 23.6 | 0.26 | 0.8 |
| Look up the key | 7.05 | 14.7 | 3.46 | 10.9 |
| Write the new record | 23.34 | 48.8 | 22.01 | 69.5 |
| Update the run counters, commit | 0.06 | 0.1 | 0.06 | 0.2 |
| Not measured (loop and call overhead) | 3.91 | 8.2 | 3.62 | 11.4 |
| **Total in Store** | **47.87** | | **31.67** | |

The remaining cost is writing the record: creating the record and its data stream and updating the unique index. A test that inserted 5,000 records with SQL instead of objects took the same time per record, so there is no cheaper way inside the current table design. Splitting a batch over several store jobs, or a different table layout, would be the next step.

## Fixed cost of a run

A rerun that reads one record, worker side, in seconds:

| Phase | Before | After |
|---|--:|--:|
| Start the read process and import PyAirbyte | 6.2 | 6 to 9 |
| Start the connector to discover the streams | 6.0 to 6.6 | 0 (cached) |
| Start the connector for the read, until its first message | 2.8 | 3.0 |
| The connector reads and stops | 5.0 | 4.6 |
| **Worker total** | **21.0 to 21.6** | **14.8** |

IRIS adds a few seconds on top (the start request, polling every 2 s, fetching the state, the acknowledgement), so the whole run takes 24 to 30 s before and 18 to 24 s after. The discovery is reused for one hour by default (`CATALOG_CACHE_TTL_SECONDS`). A column that is added in the source shows up after that time or after a call to `/discover`. The next candidates are the process start (a worker process that has imported PyAirbyte already and starts a read without the import) and the connector's own start, which the worker cannot influence.

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

## Results per scenario (first measurement, before the changes)

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

## Where the time goes now

- **Large initial load:** still the store step (317 s of 455 s for one million rows), mostly writing the records.
- **Small incremental run:** the fixed cost of a read (about 15 s in the worker, 18 s in total). Storing the records takes 3 to 5 s.
- **Unchanged data:** reading is the main cost. Storing compares hashes without opening the records.

## Recommended settings

- **`BatchSize`:** 1,000 to 5,000. The tests show no difference worth tuning for; 1,000 keeps each transaction and each HTTP call small.
- **`CallInterval` (how often the service polls):** 30 seconds or more for incremental loads, because a run now takes at least about 18 s.
- **Worker `READ_TIMEOUT_SECONDS`:** 1800 or more for reads of a million rows or more. The worker part of one million rows took 130 s here, but a slower source or network will take longer. The default of 120 is only enough for small reads.
- **`ReadTimeout` of the Extract service:** 3600 for large loads (one million rows took 12 minutes here).
- **Docker resources:** at least 4 CPUs and 6 GB of memory for loads of about a million rows (the containers together peaked at about 3.7 GB of the 4.8 GB that were available); 8 GB gives room.
- **`CATALOG_CACHE_TTL_SECONDS`:** the default of 3600 is fine. Lower it if the source tables change often; 0 turns the cache off.
- **Purge:** schedule the standard message purge. A large load creates one message per batch.

## Limits of this test

- One machine, one run per scenario (`initial_100k` twice). The same scenario differed by up to a factor of two between runs, so treat the figures as orders of magnitude, not exact values.
- Everything shares one laptop CPU. On separate hosts the store and the extraction would not compete.
- No network latency: Postgres runs next to the worker. AWS RDS results will follow with the same scripts.
- The memory and CPU figures are samples about a second apart; short peaks can be missed. CPU above 100 % means more than one core.
- The "before" and "after" runs were taken at different times on a shared laptop; "before" has three runs per scenario and one for one million rows, so differences under about 15 % are not meaningful.
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

Each run adds a row to `bench/results/results.csv`; the connection `bench` and the table `bench_items` are the only things changed. Set `BENCH_VERSION=before` (or any label) to tag the rows of a run; the default is `after`. Scenarios: `initial_10k`, `initial_100k`, `initial_1m`, `mixed_100k`, `unchanged_100k`, `unchanged_100k_full`, `recovery_100k`, `sustained_100k`, `batch_500`, `batch_2000`, `batch_5000`. The SQL for the test table is in `bench/sql/`. `Airbyte.Demo.Util.ResetConnection("bench")` clears the IRIS side of the benchmark connection; drop the table with `bench/sql/drop_items.sql`.
