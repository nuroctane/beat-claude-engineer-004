"""Back-of-envelope sizing and monthly cost for the proposed pipeline.

Every input carries its source label. Change an input, rerun, and the table
moves with it. Prices marked [Benchmarked] were checked against these pages
for us-east-1 on 2026-10-22:

  Kinesis     https://aws.amazon.com/kinesis/data-streams/pricing/
  Firehose    https://aws.amazon.com/firehose/pricing/
  Fargate     https://aws.amazon.com/fargate/pricing/
  Redis       https://docs.aws.amazon.com/AmazonElastiCache/latest/dg/CacheNodes.SupportedTypes.html
  ClickHouse  https://clickhouse.com/pricing

ClickHouse storage is sized from the measured bytes per event in
bench/RESULTS.md. Lambda, ALB, transfer, observability and export are still
my estimates, not quotes.

Usage: python cost_model.py [--json out.json]
"""

HOURS = 730  # [Assumed] hours per month

# Volume
EVENTS_PER_DAY = 50_000_000          # [Observed] from the brief
SPIKE_MULTIPLIER = 10                # [Observed] from the brief
HEADROOM = 1.5                       # [Assumed] margin on top of the spike
EVENT_BYTES = 1_000                  # [Assumed] avg JSON event incl. properties
PARQUET_RATIO = 8                    # [Assumed] JSON -> compressed Parquet
GROWTH_PER_YEAR = 2.0                # [Assumed] volume doubles in a year
PEAK_HOUR_FACTOR = 3                 # [Assumed] busiest hour vs the daily average

# Kinesis Data Streams, provisioned mode
SHARD_RECORDS_PER_S = 1_000          # [Benchmarked] Kinesis pricing page: 1,000 records/s per shard
SHARD_BYTES_PER_S = 1_000_000        # [Benchmarked] Kinesis pricing page: 1 MB/s per shard
SHARD_HOUR = 0.015                   # [Benchmarked] Kinesis pricing page
SHARD_HOUR_EXT_RETENTION = 0.020     # [Benchmarked] Kinesis pricing page, retention up to 7 days
PUT_UNIT_PER_MILLION = 0.014         # [Benchmarked] Kinesis pricing page, per 1M 25KB payload units

# Collector on Fargate, Linux/x86: per-second rates on the pricing page x 3,600
FARGATE_VCPU_HOUR = 0.04048               # [Benchmarked] Fargate pricing page
FARGATE_GB_HOUR = 0.004446                # [Benchmarked] Fargate pricing page
FARGATE_TASKS = 10                        # [Assumed] average tasks, 1 vCPU / 2 GB each
FARGATE_COLLECTOR = FARGATE_TASKS * HOURS * (FARGATE_VCPU_HOUR + 2 * FARGATE_GB_HOUR)

# Firehose bills every record rounded up to 5KB, and charges Parquet
# conversion on that same rounded volume, not on the raw bytes.
FIREHOSE_PER_GB = 0.029                   # [Benchmarked] Firehose pricing page, first 500 TB
FIREHOSE_RECORD_ROUNDING = 5_120          # [Benchmarked] Firehose pricing page, 5KB increments
FIREHOSE_PARQUET_PER_GB = 0.018           # [Benchmarked] Firehose pricing page, format conversion
S3_PER_GB_MONTH = 0.023                   # [Estimated] Standard tier; the pricing table didn't load to check

# Redis: AWS's own example prices cache.r7g.large at $0.1752/h and says Valkey
# is 20% under Redis OSS, which puts Redis OSS at about $0.219/h.
REDIS_NODE_HOUR = 0.219                   # [Estimated] derived from the ElastiCache pricing example
REDIS_NODES = 3                           # [Assumed] primary plus two replicas
REDIS = REDIS_NODES * HOURS * REDIS_NODE_HOUR
REDIS_NODE_GB = 13.07                     # [Benchmarked] ElastiCache node types, cache.r7g.large

# Estimates I couldn't check against a price list
LAMBDA = 400                              # [Estimated] ~3M batch invokes x 512MB x 1s, 4x for retries and fan-out
ALB = 300                                 # [Estimated] LCU-driven, 1.5B req/mo

# ClickHouse Cloud. Only the Enterprise rate is published for us-east-1, so it's
# the ceiling; Scale would come in under it. Sized from bench/RESULTS.md: the
# benchmark ran on 4 vCPU / 12 GiB, and each production replica gets about twice that.
CH_UNIT_HOUR = 0.3903                     # [Benchmarked] clickhouse.com/pricing, Enterprise, per 8 GiB / 2 vCPU unit
CH_UNITS_PER_REPLICA = 4                  # [Assumed] 32 GiB / 8 vCPU, about 2x the benchmark container
CH_REPLICAS = 2                           # [Assumed] two availability zones
CH_STORAGE_TB_MONTH = 25.30               # [Benchmarked] clickhouse.com/pricing, compressed TB-month
CH_BYTES_PER_EVENT = 58.9                 # [Observed] bench/RESULTS.md, compressed on disk
CH_ROLLUP_OVERHEAD = 1.15                 # [Assumed] 1-minute and per-visitor rollups on top of raw
CH_HOT_MONTHS = 12                        # [Assumed] a year of events kept queryable
CH_BACKUP_COPIES = 1                      # [Benchmarked] default: one daily backup, billed like storage
TRANSFER_NAT = 1_500                      # [Estimated] cross-AZ + NAT + egress
OBSERVABILITY = 1_000                     # [Estimated] CloudWatch + alerting
WAREHOUSE_EXPORT = 1_000                  # [Estimated] Parquet share + egress to BigQuery
OLD_PIPELINE_DURING_MIGRATION = 15_000    # [Assumed] current spend, unknown to me
BUDGET = 50_000                           # [Observed] from the brief

# Redis dedupe keyspace: one key per (tenant_id, event_id) for the dedupe window.
DEDUPE_WINDOW_S = 3_600                   # [Assumed] SDK retries land in seconds (evt-0002: 7.4s)
DEDUPE_BYTES_PER_KEY = 90                 # [Assumed] key + TTL + dict overhead


def shards_for(eps: float) -> int:
    return int(max(-(-eps // SHARD_RECORDS_PER_S), -(-eps * EVENT_BYTES // SHARD_BYTES_PER_S)))


def kinesis_cost(shards: int, events_month: float) -> float:
    return (shards * HOURS * (SHARD_HOUR + SHARD_HOUR_EXT_RETENTION)
            + events_month / 1e6 * PUT_UNIT_PER_MILLION)


def compute() -> dict:
    avg_eps = EVENTS_PER_DAY / 86_400
    design_eps = avg_eps * SPIKE_MULTIPLIER * HEADROOM
    peak_eps = design_eps * PEAK_HOUR_FACTOR
    shards, peak_shards = shards_for(design_eps), shards_for(peak_eps)
    events_month = EVENTS_PER_DAY * 30
    raw_gb_month = events_month * EVENT_BYTES / 1e9
    parquet_gb_month = raw_gb_month / PARQUET_RATIO

    kinesis = kinesis_cost(shards, events_month)
    # A 1KB event bills as 5KB. Aggregating events into fewer, larger records
    # (KPL-style) would cut this, at the cost of de-aggregating in the processor.
    firehose_billed_gb = events_month * max(EVENT_BYTES, FIREHOSE_RECORD_ROUNDING) / 1e9
    firehose_s3 = (firehose_billed_gb * (FIREHOSE_PER_GB + FIREHOSE_PARQUET_PER_GB)
                   + parquet_gb_month * 12 * S3_PER_GB_MONTH)
    ch_compute = CH_REPLICAS * CH_UNITS_PER_REPLICA * HOURS * CH_UNIT_HOUR
    ch_storage_tb = (events_month * CH_HOT_MONTHS * CH_BYTES_PER_EVENT * CH_ROLLUP_OVERHEAD
                     * (1 + CH_BACKUP_COPIES) / 1e12)
    clickhouse = ch_compute + ch_storage_tb * CH_STORAGE_TB_MONTH
    # Dedupe memory at design peak for the whole window, against a 24h window.
    dedupe_gb = design_eps * DEDUPE_WINDOW_S * DEDUPE_BYTES_PER_KEY / 1e9
    dedupe_gb_24h_peak_day = EVENTS_PER_DAY * PEAK_HOUR_FACTOR * DEDUPE_BYTES_PER_KEY / 1e9
    lines = [
        ("Kinesis Data Streams", kinesis),
        ("Collector (ECS Fargate)", FARGATE_COLLECTOR),
        ("ALB", ALB),
        ("Firehose + S3 raw archive (yr-1 steady state)", firehose_s3),
        ("Lambda stream processing", LAMBDA),
        ("ElastiCache Redis (hot profiles, dedupe)", REDIS),
        ("ClickHouse Cloud (dashboards, segments)", clickhouse),
        ("Data transfer / NAT", TRANSFER_NAT),
        ("Observability", OBSERVABILITY),
        ("Warehouse export", WAREHOUSE_EXPORT),
    ]
    new_total = sum(v for _, v in lines)
    both = new_total + OLD_PIPELINE_DURING_MIGRATION
    return {
        "sizing": {
            "avg_eps": avg_eps, "design_eps": design_eps, "peak_eps": peak_eps,
            "shards": shards, "peak_shards": peak_shards,
            "events_month": events_month, "raw_gb_month": raw_gb_month,
            "parquet_gb_month": parquet_gb_month,
            "firehose_billed_gb": firehose_billed_gb,
            "clickhouse_storage_tb": ch_storage_tb, "clickhouse_compute_usd": ch_compute,
            "dedupe_gb": dedupe_gb, "dedupe_gb_24h_peak_day": dedupe_gb_24h_peak_day,
            "redis_node_gb": REDIS_NODE_GB,
        },
        "lines": [{"name": n, "usd": round(v, 2)} for n, v in lines],
        "new_total": new_total,
        "old_pipeline": OLD_PIPELINE_DURING_MIGRATION,
        "during_migration": both,
        "budget": BUDGET,
        "headroom": BUDGET - both,
        "peak_shard_delta_usd": kinesis_cost(peak_shards, events_month) - kinesis,
        "at_2x_volume": new_total * GROWTH_PER_YEAR,
    }


def main(argv=None):
    import argparse
    import json
    ap = argparse.ArgumentParser(description="Pipeline sizing and monthly cost")
    ap.add_argument("--json", help="also write the model output as JSON")
    args = ap.parse_args(argv)
    m = compute()
    s = m["sizing"]

    print("Sizing")
    print(f"  average throughput      {s['avg_eps']:>10,.0f} events/s   [Estimated] 50M / 86,400")
    print(f"  design throughput       {s['design_eps']:>10,.0f} events/s   [Estimated] x{SPIKE_MULTIPLIER} spike x{HEADROOM} headroom")
    print(f"  Kinesis shards          {s['shards']:>10,}              [Estimated] max(records, bytes) limit")
    print(f"  if spike hits peak hour {s['peak_eps']:>10,.0f} events/s   [Estimated] x{PEAK_HOUR_FACTOR} peak-hour factor [Assumed]")
    print(f"  Kinesis shards at peak  {s['peak_shards']:>10,}              [Estimated] +${m['peak_shard_delta_usd']:,.0f}/mo")
    print(f"  events per month        {s['events_month']:>10,.0f}       [Estimated]")
    print(f"  raw GB per month        {s['raw_gb_month']:>10,.0f}              [Estimated]")
    print(f"  Parquet GB per month    {s['parquet_gb_month']:>10,.0f}              [Estimated]")
    print(f"  Firehose billed GB/mo   {s['firehose_billed_gb']:>10,.0f}              [Estimated] each 1KB record bills as 5KB")
    print(f"  Redis dedupe, 1h window {s['dedupe_gb']:>10,.1f} GB           [Estimated] at design peak, node holds ~{REDIS_NODE_GB} GB")
    print(f"  ...same with a 24h key  {s['dedupe_gb_24h_peak_day']:>10,.1f} GB           [Estimated] on a 3x day: why the window is 1h")
    print()
    print("Monthly cost (all [Estimated] unless noted)")
    for line in m["lines"]:
        print(f"  {line['name']:<48} ${line['usd']:>9,.0f}")
    print(f"  {'New pipeline total':<48} ${m['new_total']:>9,.0f}")
    print(f"  {'+ old pipeline during parallel run [Assumed]':<48} ${m['during_migration']:>9,.0f}")
    print(f"  {'Budget ceiling [Observed]':<48} ${m['budget']:>9,.0f}")
    print(f"  {'Headroom during migration':<48} ${m['headroom']:>9,.0f}")
    print(f"  {'New pipeline at 2x volume (linear, pessimistic)':<48} ${m['at_2x_volume']:>9,.0f}")
    if args.json:
        with open(args.json, "w", encoding="utf-8") as fh:
            json.dump(m, fh, indent=2)
            print(file=fh)


if __name__ == "__main__":
    main()
