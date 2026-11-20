#!/usr/bin/env python3
"""Benchmark runner for the IRIS/PyAirbyte adapter. Used through run.sh or run.ps1 (same behaviour).

    bench/run.sh initial_100k          # one scenario
    bench/run.sh list                  # scenario names
    bench/run.sh environment           # (re)write results/environment.json

It needs Python 3.8+ on the machine that runs Docker (standard library only), the compose stack running,
and a .env with POSTGRES_USER / POSTGRES_PASSWORD. Each run appends one row to results/results.csv.
The connection "bench" and the table bench_items are the only things it changes; demo data is not touched.
"""
from __future__ import annotations

import csv
import json
import os
import platform
import re
import subprocess
import sys
import threading
import time
import urllib.error
import urllib.request
from datetime import datetime, timezone
from pathlib import Path

HERE = Path(__file__).resolve().parent
RESULTS = HERE / "results"
CSV_PATH = RESULTS / "results.csv"
CONN = "bench"
ITEM = "ExtractBench"
WORKER = "http://localhost:8000"
CONTAINERS = ("worker", "iris", "postgres")  # anything else that runs during a read is the connector
DEFAULT_BATCH = 1000

COLUMNS = ["timestamp", "scenario", "rows_in_source", "batch_size", "records_read", "inserted", "updated",
           "unchanged", "rejected", "retries", "total_s", "extract_s", "store_s", "records_per_s", "mb_per_s",
           "peak_mem_mb_worker", "peak_mem_mb_iris", "peak_mem_mb_postgres", "peak_mem_mb_connector",
           "peak_cpu_pct_worker", "peak_cpu_pct_iris", "peak_cpu_pct_postgres", "peak_cpu_pct_connector",
           "staging_peak_mb", "status", "iris_rows"]


def run(cmd: list[str], stdin: str | None = None, check: bool = True, timeout: int | None = None) -> str:
    p = subprocess.run(cmd, input=stdin, capture_output=True, text=True, encoding="utf-8", errors="replace",
                       timeout=timeout)
    if check and p.returncode != 0:
        raise SystemExit(f"command failed: {' '.join(cmd[:4])} ...\n{p.stdout[-500:]}\n{p.stderr[-500:]}")
    return p.stdout


def env_file() -> dict[str, str]:
    values = {}
    path = HERE.parent / ".env"
    if path.exists():
        for line in path.read_text(encoding="utf-8").splitlines():
            if "=" in line and not line.lstrip().startswith("#"):
                k, v = line.split("=", 1)
                values[k.strip()] = v.strip()
    return values


def psql(sql_file: str | None, variables: dict[str, object] | None = None, command: str | None = None) -> str:
    env = env_file()
    cmd = ["docker", "exec", "-i", "postgres", "psql", "-q", "-t", "-A", "-v", "ON_ERROR_STOP=1",
           "-U", env.get("POSTGRES_USER", "demo"), "-d", env.get("POSTGRES_DB", "demo")]
    for k, v in (variables or {}).items():
        cmd += ["-v", f"{k}={v}"]
    if command:
        return run(cmd + ["-c", command])
    return run(cmd, stdin=(HERE / "sql" / sql_file).read_text(encoding="utf-8"))


def source_rows() -> int:
    try:
        return int(psql(None, command="SELECT count(*) FROM bench_items").strip())
    except SystemExit:
        return 0


def iris(script: str, timeout: int | None = None) -> str:
    return run(["docker", "exec", "-i", "iris", "iris", "session", "IRIS", "-U", "USER"], stdin=script + "\nhalt\n",
               timeout=timeout)


def iris_rows() -> int:
    out = iris('set rs=##class(%SQL.Statement).%ExecDirect(,"SELECT COUNT(*) AS n FROM Airbyte_Demo_Data.Record '
               f"WHERE ConnectionId='{CONN}'\")\ndo rs.%Next() write \"ROWS|\",rs.n,!")
    m = re.search(r"ROWS\|(\d+)", out)
    return int(m.group(1)) if m else -1


def reset_connection() -> None:
    iris(f'do ##class(Airbyte.Demo.Util).ResetConnection("{CONN}")')


def set_item(enabled: bool) -> None:
    iris(f'set sc=##class(Ens.Director).EnableConfigItem("{ITEM}",{1 if enabled else 0},1)')


def create_table(n: int) -> None:
    psql("create_items.sql", {"n": n})


def mixed_change(upd: int, ins: int, seed: int = 0) -> None:
    psql("mixed_change.sql", {"upd_pct": upd, "ins_pct": ins, "seed": seed})


# ---- measuring -------------------------------------------------------------------------------------------

def mib(text: str) -> float:
    m = re.match(r"([\d.]+)\s*([KMGT]?i?B)", text.strip())
    if not m:
        return 0.0
    scale = {"B": 1 / 1048576, "KiB": 1 / 1024, "kB": 1 / 1024, "MiB": 1, "MB": 1, "GiB": 1024, "GB": 1024}
    return float(m.group(1)) * scale.get(m.group(2), 1)


class Sampler:
    """Samples docker stats in a loop (one pass takes about a second) and keeps peak memory and CPU per container,
    plus the peak size of the worker's read folders."""

    def __init__(self) -> None:
        self.peak_mem: dict[str, float] = {}
        self.peak_cpu: dict[str, float] = {}
        self.staging_mb = 0.0
        self.samples = 0
        self._stop = threading.Event()
        self._threads = [threading.Thread(target=self._stats, daemon=True),
                         threading.Thread(target=self._staging, daemon=True)]

    def _stats(self) -> None:
        while not self._stop.is_set():
            out = run(["docker", "stats", "--no-stream", "--format", "{{.Name}}|{{.CPUPerc}}|{{.MemUsage}}"],
                      check=False)
            for line in out.splitlines():
                parts = line.split("|")
                if len(parts) != 3:
                    continue
                name = parts[0] if parts[0] in CONTAINERS else "connector"
                cpu = float(parts[1].rstrip("%") or 0)
                mem = mib(parts[2].split("/")[0])
                self.peak_cpu[name] = max(self.peak_cpu.get(name, 0.0), cpu)
                self.peak_mem[name] = max(self.peak_mem.get(name, 0.0), mem)
            self.samples += 1

    def _staging(self) -> None:
        while not self._stop.is_set():
            out = run(["docker", "exec", "worker", "du", "-sk", "/data/reads"], check=False)
            if out.split():
                self.staging_mb = max(self.staging_mb, int(out.split()[0]) / 1024)
            self._stop.wait(1)

    def __enter__(self) -> "Sampler":
        for t in self._threads:
            t.start()
        return self

    def __exit__(self, *exc: object) -> None:
        self._stop.set()
        for t in self._threads:
            t.join(timeout=15)


def worker_json(path: str, method: str = "GET") -> dict:
    req = urllib.request.Request(WORKER + path, method=method)
    try:
        with urllib.request.urlopen(req, timeout=10) as r:
            return json.loads(r.read())
    except urllib.error.HTTPError as e:
        return json.loads(e.read() or b"{}")
    except (urllib.error.URLError, OSError):
        return {}


def read_ids() -> set[str]:
    return set(run(["docker", "exec", "worker", "ls", "/data/reads"], check=False).split())


def newest_read(before: set[str]) -> str | None:
    new = sorted(read_ids() - before)
    return new[-1] if new else None


def worker_read_stats(read_id: str | None) -> tuple[float, int]:
    if not read_id:
        return 0.0, 0
    st = worker_json(f"/reads/{read_id}")
    return float(st.get("duration_seconds") or 0), int(st.get("bytes_produced") or 0)


def last_run() -> dict:
    out = iris('set rs=##class(%SQL.Statement).%ExecDirect(,"SELECT TOP 1 ID,Status,RecordsRead,Inserted,Updated,'
               "Unchanged,Rejected,RetryCount,DurationSeconds,StoreSeconds FROM Airbyte_Demo_Data.Run "
               f"WHERE ConnectionId='{CONN}' ORDER BY ID DESC\")\n"
               'do rs.%Next() write "RUN|",rs.ID,"|",rs.Status,"|",rs.RecordsRead,"|",rs.Inserted,"|",rs.Updated,'
               '"|",rs.Unchanged,"|",rs.Rejected,"|",rs.RetryCount,"|",rs.DurationSeconds,"|",rs.StoreSeconds,!')
    m = re.search(r"RUN\|(.*)", out)
    if not m:
        return {}
    f = m.group(1).strip().split("|")
    keys = ["id", "status", "read", "inserted", "updated", "unchanged", "rejected", "retries", "total", "store"]
    return dict(zip(keys, f))


def execute(label: str, batch: int, max_seconds: int | None = None, cancel_after_batches: int | None = None, mode: str = "") -> dict:
    """One run through IRIS with sampling. Optionally cancel the worker's read after some batches (recovery
    test) or when max_seconds is over. mode overrides the Mode setting for this run ("full_refresh"). Returns the values for one CSV row."""
    before = read_ids()
    set_item(True)
    script = (f'set sc=##class(Airbyte.Demo.Service.Extract).RunNow(0,"{ITEM}",{batch},"{mode}")\nhalt\n')
    sampler = Sampler()
    note = ""
    t0 = time.monotonic()
    with sampler:
        proc = subprocess.Popen(["docker", "exec", "-i", "iris", "iris", "session", "IRIS", "-U", "USER"],
                                stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
        assert proc.stdin
        proc.stdin.write(script)
        proc.stdin.flush()
        read_id = None
        cancelled = False
        while proc.poll() is None:
            time.sleep(1)
            read_id = read_id or newest_read(before)
            if read_id and not cancelled:
                over = max_seconds is not None and time.monotonic() - t0 > max_seconds
                half = (cancel_after_batches is not None
                        and int(worker_json(f"/reads/{read_id}").get("batches_ready") or 0) >= cancel_after_batches)
                if over or half:
                    answer = worker_json(f"/reads/{read_id}", "DELETE")
                    cancelled = True
                    if answer.get("status") != "cancelled":
                        continue  # the read had already finished: nothing was interrupted
                    note = "stopped over the time limit" if over else "read cancelled on purpose"
        proc.communicate()
        wall = time.monotonic() - t0
    set_item(False)
    extract_s, nbytes = worker_read_stats(read_id)
    r = last_run()
    total = float(r.get("total") or wall)
    # a run that failed reports 0 read; count what was stored so the row is still meaningful
    read = max(int(r.get("read") or 0), sum(int(r.get(k) or 0) for k in ("inserted", "updated", "unchanged", "rejected")))
    status = r.get("status", "unknown")
    if note:
        status = f"{status} ({note})"
    row = {
        "timestamp": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"), "scenario": label,
        "rows_in_source": source_rows(), "batch_size": batch, "records_read": read,
        "inserted": r.get("inserted", 0), "updated": r.get("updated", 0), "unchanged": r.get("unchanged", 0),
        "rejected": r.get("rejected", 0), "retries": r.get("retries", 0), "total_s": round(total, 2),
        "extract_s": round(extract_s, 2), "store_s": round(float(r.get("store") or 0), 2),
        "records_per_s": round(read / total, 1) if total else 0,
        "mb_per_s": round(nbytes / 1e6 / total, 2) if total else 0,
        "staging_peak_mb": round(sampler.staging_mb, 1), "status": status, "iris_rows": iris_rows(),
    }
    for c in ("worker", "iris", "postgres", "connector"):
        row[f"peak_mem_mb_{c}"] = round(sampler.peak_mem.get(c, 0), 0)
        row[f"peak_cpu_pct_{c}"] = round(sampler.peak_cpu.get(c, 0), 0)
    append(row)
    print(f"{label}: {status} read={read} inserted={row['inserted']} updated={row['updated']} "
          f"unchanged={row['unchanged']} total={row['total_s']}s extract={row['extract_s']}s "
          f"store={row['store_s']}s {row['records_per_s']} rec/s iris_rows={row['iris_rows']}")
    return row


def append(row: dict) -> None:
    RESULTS.mkdir(exist_ok=True)
    new = not CSV_PATH.exists()
    with open(CSV_PATH, "a", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=COLUMNS)
        if new:
            w.writeheader()
        w.writerow({k: row.get(k, "") for k in COLUMNS})


# ---- scenarios -------------------------------------------------------------------------------------------

def fresh(n: int) -> None:
    """Source table with n rows and an empty bench connection."""
    create_table(n)
    reset_connection()


def initial(name: str, n: int, batch: int = DEFAULT_BATCH, max_seconds: int | None = None) -> None:
    fresh(n)
    execute(name, batch, max_seconds=max_seconds)


def loaded_100k() -> None:
    fresh(100_000)
    r = execute("setup_load_100k (not part of the results)", DEFAULT_BATCH)
    if int(r["inserted"]) != 100_000:
        raise SystemExit("setup load did not insert 100000 rows; stopping")
    remove_last_row()


def remove_last_row() -> None:
    """The setup load is not a result: take its row back out of results.csv."""
    lines = CSV_PATH.read_text(encoding="utf-8").splitlines()
    CSV_PATH.write_text("\n".join(lines[:-1]) + "\n", encoding="utf-8")


def mixed_100k() -> None:
    loaded_100k()
    mixed_change(10, 5)
    execute("mixed_100k", DEFAULT_BATCH)


def unchanged_100k_full() -> None:
    """Same unchanged data, but read in full (no cursor): every record is read and compared by hash."""
    loaded_100k()
    execute("unchanged_100k_full_refresh", DEFAULT_BATCH, mode="full_refresh")


def unchanged_100k() -> None:
    loaded_100k()
    execute("unchanged_100k", DEFAULT_BATCH)


def recovery_100k() -> None:
    fresh(100_000)
    execute("recovery_100k_interrupted", DEFAULT_BATCH, cancel_after_batches=50)
    r = execute("recovery_100k_rerun", DEFAULT_BATCH)
    ok = r["iris_rows"] == r["rows_in_source"]
    print(f"recovery check: IRIS rows {r['iris_rows']} vs source rows {r['rows_in_source']} -> "
          f"{'equal, no duplicates' if ok else 'MISMATCH'}")


def sustained_100k() -> None:
    loaded_100k()
    for i in range(1, 11):
        mixed_change(5, 0, seed=i * 7)
        execute(f"sustained_100k_{i:02d}", DEFAULT_BATCH)


SCENARIOS = {
    "initial_10k": lambda: initial("initial_10k", 10_000),
    "initial_100k": lambda: initial("initial_100k", 100_000),
    "initial_1m": lambda: initial("initial_1m", 1_000_000, max_seconds=1200),
    "mixed_100k": mixed_100k,
    "unchanged_100k": unchanged_100k,
    "unchanged_100k_full": unchanged_100k_full,
    "recovery_100k": recovery_100k,
    "sustained_100k": sustained_100k,
    "batch_500": lambda: initial("batch_500", 100_000, 500),
    "batch_2000": lambda: initial("batch_2000", 100_000, 2000),
    "batch_5000": lambda: initial("batch_5000", 100_000, 5000),
}


# ---- environment -----------------------------------------------------------------------------------------

def cpu_model() -> str:
    if platform.system() == "Windows":
        out = run(["powershell", "-NoProfile", "-Command", "(Get-CimInstance Win32_Processor).Name"], check=False)
        return out.strip().splitlines()[0] if out.strip() else platform.processor()
    try:
        for line in Path("/proc/cpuinfo").read_text().splitlines():
            if line.startswith("model name"):
                return line.split(":", 1)[1].strip()
    except OSError:
        pass
    return platform.processor()


def os_name() -> str:
    # Python reports Windows 11 as "Windows 10"; builds from 22000 on are Windows 11
    if platform.system() == "Windows":
        build = int(platform.version().split(".")[-1])
        return f"Windows {11 if build >= 22000 else platform.release()} (build {build})"
    return f"{platform.system()} {platform.release()}"


def write_environment() -> None:
    RESULTS.mkdir(exist_ok=True)
    info = json.loads(run(["docker", "info", "--format", "{{json .}}"]))
    out = iris('write "VER|",$ZVersion,!\nwrite "CPF|",##class(%File).Exists("/usr/irissys/iris.cpf"),!')
    ver = re.search(r"VER\|(.*)", out)
    cpf = run(["docker", "exec", "iris", "sh", "-c", "grep -i '^globals=\\|^routines=\\|^gmheap=' /usr/irissys/iris.cpf"],
              check=False).split()
    host_ram = ""
    if platform.system() == "Windows":
        host_ram = run(["powershell", "-NoProfile", "-Command",
                        "[math]::Round((Get-CimInstance Win32_ComputerSystem).TotalPhysicalMemory/1GB,1)"],
                       check=False).strip()
    env = {
        "written": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "host": {"cpu": cpu_model(), "logical_cores": os.cpu_count(), "ram_gb": host_ram,
                 "os": os_name()},
        "docker": {"server_version": info.get("ServerVersion"), "operating_system": info.get("OperatingSystem"),
                   "cpus_available_to_containers": info.get("NCPU"),
                   "memory_available_to_containers_gb": round(info.get("MemTotal", 0) / 1024 ** 3, 1),
                   "kernel": info.get("KernelVersion")},
        "iris": {"version": ver.group(1).strip() if ver else "", "buffers_cpf": cpf,
                 "note": "globals=0,... means automatic buffer sizing (the default)"},
        "worker": worker_json("/env"),
        "postgres_image": run(["docker", "inspect", "-f", "{{.Config.Image}}", "postgres"], check=False).strip(),
        "batch_size_default": DEFAULT_BATCH,
    }
    (RESULTS / "environment.json").write_text(json.dumps(env, indent=2), encoding="utf-8")
    print("wrote", RESULTS / "environment.json")


def main() -> None:
    if len(sys.argv) != 2 or sys.argv[1] in ("-h", "--help"):
        print(__doc__)
        raise SystemExit(1)
    name = sys.argv[1]
    if name == "list":
        print("\n".join(list(SCENARIOS) + ["environment"]))
        return
    if name == "environment":
        write_environment()
        return
    if name not in SCENARIOS:
        raise SystemExit(f"unknown scenario '{name}'. Known: {', '.join(SCENARIOS)}")
    if not (RESULTS / "environment.json").exists():
        write_environment()
    SCENARIOS[name]()


if __name__ == "__main__":
    main()
