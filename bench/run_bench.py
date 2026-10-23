"""Benchmark the riskiest component: ClickHouse at one day of this brief's volume.

Runs a pinned ClickHouse in Docker, capped at 4 vCPU / 12 GiB (about two
ClickHouse Cloud compute units), and measures what the design leans on:

  1. bulk load rate and compressed bytes per event (storage cost)
  2. insert-to-queryable latency with async inserts at the design peak rate
  3. dashboard and segment query latency, 1 and 16 concurrent users
  4. whether ReplacingMergeTree actually collapses SDK retries, and what the
     1-minute rollup counts before it does

Synthetic data, shaped like the fixture: ~500 tenants with a heavy skew,
200k visitors per tenant, 3% bots, 0.5% retries arriving 2 to 10s late.

Stdlib only, plus Docker. Writes bench/results.json and bench/RESULTS.md.

Usage: python bench/run_bench.py [--rows 50000000] [--keep] [--report]
"""

from __future__ import annotations

import argparse
import json
import re
import statistics
import subprocess
import threading
import time
import urllib.parse
import urllib.request
import uuid
from datetime import datetime, timezone
from pathlib import Path

HERE = Path(__file__).resolve().parent
IMAGE = "clickhouse/clickhouse-server:24.8"
NAME = "e004-bench"
CPUS, MEMORY = "4", "12g"
PORT = 18123
USER = PASSWORD = "bench"
DESIGN_PEAK_EPS = 8_681          # from cost_model.py: 50M/day x 10 spike x 1.5 headroom

SCHEMA = [
    """CREATE TABLE events (
        tenant_id    LowCardinality(String),
        event_id     String,
        anonymous_id String,
        user_id      Nullable(String),
        type         LowCardinality(String),
        path         LowCardinality(String),
        ts           DateTime64(3, 'UTC'),
        received_at  DateTime64(3, 'UTC'),
        is_bot       UInt8
    ) ENGINE = ReplacingMergeTree
    PARTITION BY toYYYYMMDD(received_at)
    ORDER BY (tenant_id, toStartOfHour(received_at), event_id)""",
    """CREATE TABLE events_1m (
        tenant_id LowCardinality(String),
        minute    DateTime('UTC'),
        type      LowCardinality(String),
        path      LowCardinality(String),
        events    SimpleAggregateFunction(sum, UInt64),
        visitors  AggregateFunction(uniq, String)
    ) ENGINE = AggregatingMergeTree
    ORDER BY (tenant_id, minute, type, path)""",
    """CREATE MATERIALIZED VIEW events_1m_mv TO events_1m AS
    SELECT tenant_id, toStartOfMinute(received_at) AS minute, type, path,
           count() AS events, uniqState(anonymous_id) AS visitors
    FROM events WHERE is_bot = 0
    GROUP BY tenant_id, minute, type, path""",
    # Segments like "viewed /pricing 3+ times today" scan the raw table badly under
    # concurrency, so they read per-visitor daily page counts instead.
    """CREATE TABLE visitor_day (
        tenant_id    LowCardinality(String),
        day          Date,
        path         LowCardinality(String),
        anonymous_id String,
        views        SimpleAggregateFunction(sum, UInt64)
    ) ENGINE = AggregatingMergeTree
    ORDER BY (tenant_id, day, path, anonymous_id)""",
    """CREATE MATERIALIZED VIEW visitor_day_mv TO visitor_day AS
    SELECT tenant_id, toDate(received_at) AS day, path, anonymous_id, count() AS views
    FROM events WHERE is_bot = 0 AND type = 'page_view'
    GROUP BY tenant_id, day, path, anonymous_id""",
]

GENERATE = """
INSERT INTO events
SELECT
    concat('t-', leftPad(toString(tn), 3, '0')) AS tenant_id,
    toString(generateUUIDv4()) AS event_id,
    concat('anon-', toString(cityHash64(tn, rand32(1) % 200000) % 100000000)) AS anonymous_id,
    if(rand32(2) % 5 = 0, concat('u-', toString(cityHash64(anonymous_id) % 10000000)), NULL) AS user_id,
    ['page_view','page_view','page_view','click','click','form_submit','custom','identify'][rand32(3) % 8 + 1] AS type,
    ['/','/pricing','/features','/docs','/blog','/about','/signup','/dashboard'][rand32(4) % 8 + 1] AS path,
    received_at - toIntervalMillisecond(rand32(5) % 300) AS ts,
    received_at,
    rand32(6) % 100 < 3 AS is_bot
FROM (
    SELECT toUInt32(floor(pow(500, randUniform(0, 1)))) AS tn,
           toDateTime64('2026-06-15 00:00:00', 3, 'UTC') + toIntervalMillisecond(rand64(7) % 86400000) AS received_at
    FROM numbers({rows})
)"""

# SDK retries: same event, received 2 to 10s later.
RETRIES = """
INSERT INTO events
SELECT tenant_id, event_id, anonymous_id, user_id, type, path, ts,
       received_at + toIntervalMillisecond(2000 + rand32(1) % 8000), is_bot
FROM events WHERE cityHash64(event_id) % 200 = 0"""

TENANTS = ["t-001", "t-005", "t-040", "t-250"]
QUERIES = {
    "dashboard: events per minute, last 15 min (rollup)":
        "SELECT minute, type, sum(events) FROM events_1m WHERE tenant_id = '{t}' "
        "AND minute >= toDateTime('2026-06-15 23:45:00', 'UTC') GROUP BY minute, type ORDER BY minute",
    "dashboard: top pages and visitors, last hour (rollup)":
        "SELECT path, sum(events) AS views, uniqMerge(visitors) AS visitors FROM events_1m "
        "WHERE tenant_id = '{t}' AND minute >= toDateTime('2026-06-15 23:00:00', 'UTC') "
        "GROUP BY path ORDER BY views DESC LIMIT 10",
    "segment: viewed /pricing 3+ times today (raw)":
        "SELECT count() FROM (SELECT anonymous_id FROM events WHERE tenant_id = '{t}' "
        "AND received_at >= toDateTime64('2026-06-15 00:00:00', 3, 'UTC') "
        "AND received_at < toDateTime64('2026-06-16 00:00:00', 3, 'UTC') "
        "AND type = 'page_view' AND path = '/pricing' AND is_bot = 0 "
        "GROUP BY anonymous_id HAVING count() >= 3)",
    "segment: viewed /pricing 3+ times today (per-visitor rollup)":
        "SELECT count() FROM (SELECT anonymous_id FROM visitor_day WHERE tenant_id = '{t}' "
        "AND day = toDate('2026-06-15') AND path = '/pricing' "
        "GROUP BY anonymous_id HAVING sum(views) >= 3)",
    "reconciliation: exact hourly count with FINAL (raw)":
        "SELECT count() FROM events FINAL WHERE tenant_id = '{t}' "
        "AND received_at >= toDateTime64('2026-06-15 12:00:00', 3, 'UTC') "
        "AND received_at < toDateTime64('2026-06-15 13:00:00', 3, 'UTC')",
}


def sh(*args, input=None, check=True):
    return subprocess.run(args, input=input, capture_output=True, text=True, check=check)


def q(sql: str, data: bytes | None = None, timeout: float = 1800, **settings) -> str:
    params = {"user": USER, "password": PASSWORD, **settings}
    if data is None:
        body = sql.encode()
    else:
        params["query"] = sql
        body = data
    # 127.0.0.1, not localhost: on Windows, localhost tries IPv6 first, Docker drops it,
    # and every request eats the 21s TCP connect timeout before falling back.
    url = f"http://127.0.0.1:{PORT}/?" + urllib.parse.urlencode(params)
    with urllib.request.urlopen(urllib.request.Request(url, data=body, method="POST"),
                                timeout=timeout) as resp:
        return resp.read().decode().strip()


def start() -> None:
    sh("docker", "rm", "-f", NAME, check=False)
    sh("docker", "run", "-d", "--name", NAME, f"--cpus={CPUS}", f"--memory={MEMORY}",
       "--ulimit", "nofile=262144:262144", "-p", f"{PORT}:8123",
       "-e", f"CLICKHOUSE_USER={USER}", "-e", f"CLICKHOUSE_PASSWORD={PASSWORD}", IMAGE)
    for _ in range(120):
        try:
            if q("SELECT 1", timeout=2) == "1":
                return
        except OSError:
            pass
        time.sleep(1)
    raise SystemExit("ClickHouse did not come up")


def bulk_load(rows: int) -> dict:
    for stmt in SCHEMA:
        q(stmt)
    t0 = time.perf_counter()
    q(GENERATE.format(rows=rows), max_insert_threads=4, max_threads=4)
    load_s = time.perf_counter() - t0
    q(RETRIES)
    parts = q("SELECT sum(rows), sum(data_compressed_bytes), sum(data_uncompressed_bytes) "
              "FROM system.parts WHERE table = 'events' AND active", default_format="TSV").split("\t")
    raw_rows, comp, uncomp = (int(x) for x in parts)
    return {"rows_generated": rows, "load_seconds": round(load_s, 1),
            "load_rows_per_s": round(rows / load_s), "rows_with_retries": raw_rows,
            "compressed_bytes": comp, "uncompressed_bytes": uncomp,
            "compressed_bytes_per_event": round(comp / raw_rows, 1)}


def dedupe() -> dict:
    def n(sql):
        return int(q(sql))
    unique = n("SELECT uniqExact(event_id) FROM events")
    raw = n("SELECT count() FROM events")
    final_rows = n("SELECT count() FROM events FINAL")
    unique_human = n("SELECT uniqExact(event_id) FROM events WHERE is_bot = 0")
    rollup = n("SELECT sum(events) FROM events_1m")
    return {"unique_events": unique, "rows_before_merge": raw, "retries_injected": raw - unique,
            "rows_with_final": final_rows, "retries_surviving_final": final_rows - unique,
            "rollup_events": rollup, "unique_non_bot_events": unique_human,
            "rollup_overcount": rollup - unique_human}


def tenant_sizes() -> dict:
    out = q("SELECT tenant_id, count() FROM events WHERE tenant_id IN ("
            + ",".join(f"'{t}'" for t in TENANTS) + ") GROUP BY tenant_id ORDER BY tenant_id",
            default_format="TSV")
    return {t: int(c) for t, c in (line.split("\t") for line in out.splitlines())}


PCT = re.compile(r"^\s*(\d+(?:\.\d+)?)%\s+([\d.]+) sec", re.M)
QPS = re.compile(r"QPS:\s*([\d.]+)")


def bench_query(sql_template: str, concurrency: int, seconds: int) -> dict:
    lines = "\n".join(sql_template.format(t=t) for t in TENANTS) + "\n"
    res = sh("docker", "exec", "-i", NAME, "clickhouse-benchmark", "--user", USER,
             "--password", PASSWORD, "-c", str(concurrency), "-t", str(seconds), "-d", "0",
             input=lines)
    text = res.stdout + res.stderr
    pct = {float(p): float(s) for p, s in PCT.findall(text)}
    qps = QPS.findall(text)
    return {"concurrency": concurrency, "qps": float(qps[-1]) if qps else None,
            "p50_ms": round(pct.get(50.0, float("nan")) * 1000, 1),
            "p95_ms": round(pct.get(95.0, float("nan")) * 1000, 1),
            "p99_ms": round(pct.get(99.0, float("nan")) * 1000, 1)}


def stream(seconds: int, threads: int, batch: int) -> dict:
    """Async inserts at about the design peak; latency = send to acknowledged flush,
    which with wait_for_async_insert=1 is when rows (and the rollup) are queryable."""
    latencies, rows_sent, errors = [], [0], [0]
    lock = threading.Lock()
    stop = time.perf_counter() + seconds
    sql = "INSERT INTO events FORMAT JSONEachRow"

    def worker(seed: int):
        tenants = [f"t-{n:03d}" for n in (1, 2, 3, 5, 8, 13, 40, 120, 250, 400)]
        i = 0
        while time.perf_counter() < stop:
            now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
            body = "\n".join(json.dumps({
                "tenant_id": tenants[(seed + i + k) % len(tenants)], "event_id": str(uuid.uuid4()),
                "anonymous_id": f"anon-{(seed * 7919 + i * 31 + k) % 200000}", "user_id": None,
                "type": "page_view", "path": "/pricing", "ts": now, "received_at": now, "is_bot": 0,
            }) for k in range(batch)).encode()
            t0 = time.perf_counter()
            try:
                q(sql, data=body, timeout=30, async_insert=1, wait_for_async_insert=1,
                  async_insert_busy_timeout_ms=1000)
                dt = time.perf_counter() - t0
                with lock:
                    latencies.append(dt)
                    rows_sent[0] += batch
            except OSError:
                with lock:
                    errors[0] += 1
            i += 1

    t_start = time.perf_counter()
    pool = [threading.Thread(target=worker, args=(s,)) for s in range(threads)]
    for t in pool:
        t.start()
    for t in pool:
        t.join()
    elapsed = time.perf_counter() - t_start
    latencies.sort()
    pick = lambda p: round(latencies[min(len(latencies) - 1, int(p * len(latencies)))] * 1000)
    return {"seconds": round(elapsed, 1), "threads": threads, "batch_rows": batch,
            "rows_per_s": round(rows_sent[0] / elapsed), "target_rows_per_s": DESIGN_PEAK_EPS,
            "requests": len(latencies), "errors": errors[0],
            "ack_p50_ms": pick(0.50), "ack_p95_ms": pick(0.95), "ack_p99_ms": pick(0.99),
            "ack_mean_ms": round(statistics.mean(latencies) * 1000) if latencies else None}


def write_markdown(r: dict) -> str:
    L, D, S, Q = r["load"], r["dedupe"], r["stream"], r["queries"]
    p95 = lambda name, users: next(x["p95_ms"] for x in Q[name] if x["concurrency"] == users)
    raw_seg = p95("segment: viewed /pricing 3+ times today (raw)", 16)
    roll_seg = p95("segment: viewed /pricing 3+ times today (per-visitor rollup)", 16)
    out = [
        "# ClickHouse benchmark",
        "",
        f"ClickHouse {r['clickhouse_version']} in Docker, capped at {CPUS} vCPU and {MEMORY.upper()[:-1]} GiB, "
        f"which is about two ClickHouse Cloud compute units. Run {r['ran_at']} on "
        f"{L['rows_generated']:,} synthetic events shaped like the fixture: about 500 tenants with a "
        "heavy skew, 3% bots and 0.5% SDK retries arriving 2 to 10s late.",
        "",
        "It's one replica on local SSD with one day of data. Cloud object storage and a year of "
        "history will be slower, so read these as a floor. Reproduce with `python bench/run_bench.py`.",
        "",
        "## What it found",
        "",
        f"- Async inserts held {S['rows_per_s']:,} events per second for {S['seconds']}s against the "
        f"{S['target_rows_per_s']:,} design peak, with {S['errors']} errors. Rows and rollups were "
        f"queryable {S['ack_p95_ms']:,} ms after the send at p95.",
        f"- Dashboard tiles read from the 1-minute rollup. At 16 concurrent users they ran "
        f"{p95('dashboard: events per minute, last 15 min (rollup)', 16):,.0f} and "
        f"{p95('dashboard: top pages and visitors, last hour (rollup)', 16):,.0f} ms at p95.",
        f"- A segment that scans the raw table doesn't hold up under concurrency, with a p95 of "
        f"{raw_seg:,.0f} ms at 16 users. Reading per-visitor daily counts instead brings it to "
        f"{roll_seg:,.0f} ms.",
        f"- {D['retries_surviving_final']:,} of {D['retries_injected']:,} retries survived "
        "`FINAL`. They straddled the top of an hour, so their sorting key differed. Reconciliation "
        "counts distinct `event_id` for that reason.",
        f"- The 1-minute rollup counted {D['rollup_overcount']:,} retries. A materialized view sees "
        "every insert before any merge, so dedupe has to happen in Redis, before ClickHouse.",
        "",
        "## Load and storage",
        "",
        f"- {L['compressed_bytes_per_event']} compressed bytes per event on disk, "
        f"{L['compressed_bytes'] / 1e9:.2f} GB for the day.",
        f"- The bulk load took {L['load_seconds']}s, generation included. That's not the production "
        "path; the async insert test is.",
        f"- Tenant skew in rows: {', '.join(f'{t} {n:,}' for t, n in r['tenant_rows'].items())}.",
        "",
        "## Query latency",
        "",
        "| Query | Users | QPS | p50 ms | p95 ms | p99 ms |",
        "|---|---:|---:|---:|---:|---:|",
    ]
    for name, runs in Q.items():
        for run in runs:
            out.append(f"| {name} | {run['concurrency']} | {run['qps']} | {run['p50_ms']} | "
                       f"{run['p95_ms']} | {run['p99_ms']} |")
    out.append("")
    return "\n".join(out)


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--rows", type=int, default=50_000_000)
    ap.add_argument("--keep", action="store_true", help="leave the container running")
    ap.add_argument("--report", action="store_true",
                    help="rewrite RESULTS.md from results.json without running anything")
    args = ap.parse_args()
    if args.report:
        result = json.loads((HERE / "results.json").read_text(encoding="utf-8"))
        (HERE / "RESULTS.md").write_text(write_markdown(result), encoding="utf-8")
        print("wrote bench/RESULTS.md")
        return
    print(f"starting {IMAGE} ({CPUS} vCPU, {MEMORY})", flush=True)
    start()
    try:
        result = {"ran_at": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC"),
                  "clickhouse_version": q("SELECT version()"),
                  "container": {"cpus": CPUS, "memory": MEMORY}}
        print(f"loading {args.rows:,} events", flush=True)
        result["load"] = bulk_load(args.rows)
        print(json.dumps(result["load"]), flush=True)
        result["tenant_rows"] = tenant_sizes()
        result["dedupe"] = dedupe()
        print(json.dumps(result["dedupe"]), flush=True)
        result["queries"] = {}
        for name, sql in QUERIES.items():
            print(f"query: {name}", flush=True)
            result["queries"][name] = [bench_query(sql, 1, 8), bench_query(sql, 16, 12)]
            print(json.dumps(result["queries"][name]), flush=True)
        print("streaming at the design peak", flush=True)
        result["stream"] = stream(seconds=45, threads=12, batch=750)
        print(json.dumps(result["stream"]), flush=True)
    finally:
        if not args.keep:
            sh("docker", "rm", "-f", NAME, check=False)
    (HERE / "results.json").write_text(json.dumps(result, indent=2) + "\n", encoding="utf-8")
    (HERE / "RESULTS.md").write_text(write_markdown(result), encoding="utf-8")
    print("wrote bench/results.json and bench/RESULTS.md")


if __name__ == "__main__":
    main()
