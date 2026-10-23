# ClickHouse benchmark

ClickHouse 24.8.14.39 in Docker, capped at 4 vCPU and 12 GiB, which is about two ClickHouse Cloud compute units. Run 2026-10-22 22:19 UTC on 50,000,000 synthetic events shaped like the fixture: about 500 tenants with a heavy skew, 3% bots and 0.5% SDK retries arriving 2 to 10s late.

It's one replica on local SSD with one day of data. Cloud object storage and a year of history will be slower, so read these as a floor. Reproduce with `python bench/run_bench.py`.

## What it found

- Async inserts held 8,825 events per second for 46.1s against the 8,681 design peak, with 0 errors. Rows and rollups were queryable 1,197 ms after the send at p95.
- Dashboard tiles read from the 1-minute rollup. At 16 concurrent users they ran 117 and 306 ms at p95.
- A segment that scans the raw table doesn't hold up under concurrency, with a p95 of 2,398 ms at 16 users. Reading per-visitor daily counts instead brings it to 422 ms.
- 423 of 249,339 retries survived `FINAL`. They straddled the top of an hour, so their sorting key differed. Reconciliation counts distinct `event_id` for that reason.
- The 1-minute rollup counted 242,007 retries. A materialized view sees every insert before any merge, so dedupe has to happen in Redis, before ClickHouse.

## Load and storage

- 58.9 compressed bytes per event on disk, 2.96 GB for the day.
- The bulk load took 286.9s, generation included. That's not the production path; the async insert test is.
- Tenant skew in rows: t-001 5,601,327, t-005 1,475,237, t-040 198,994, t-250 32,274.

## Query latency

| Query | Users | QPS | p50 ms | p95 ms | p99 ms |
|---|---:|---:|---:|---:|---:|
| dashboard: events per minute, last 15 min (rollup) | 1 | 124.953 | 7.0 | 9.0 | 10.0 |
| dashboard: events per minute, last 15 min (rollup) | 16 | 161.973 | 100.0 | 117.0 | 185.0 |
| dashboard: top pages and visitors, last hour (rollup) | 1 | 65.9 | 12.0 | 28.0 | 31.0 |
| dashboard: top pages and visitors, last hour (rollup) | 16 | 78.846 | 200.0 | 306.0 | 326.0 |
| segment: viewed /pricing 3+ times today (raw) | 1 | 24.612 | 30.0 | 103.0 | 120.0 |
| segment: viewed /pricing 3+ times today (raw) | 16 | 15.283 | 724.0 | 2398.0 | 2713.0 |
| segment: viewed /pricing 3+ times today (per-visitor rollup) | 1 | 69.715 | 11.0 | 27.0 | 29.0 |
| segment: viewed /pricing 3+ times today (per-visitor rollup) | 16 | 59.225 | 216.0 | 422.0 | 519.0 |
| reconciliation: exact hourly count with FINAL (raw) | 1 | 39.739 | 22.0 | 49.0 | 55.0 |
| reconciliation: exact hourly count with FINAL (raw) | 16 | 33.148 | 418.0 | 822.0 | 940.0 |
