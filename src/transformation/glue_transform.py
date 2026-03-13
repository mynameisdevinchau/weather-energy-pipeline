import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.context import SparkContext
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, IntegerType
from datetime import datetime, timedelta

# ── Initialize Glue + Spark ───────────────────────────────────────────────────
args = getResolvedOptions(sys.argv, ["JOB_NAME", "S3_BUCKET", "START_DATE", "END_DATE"])

sc          = SparkContext()
glueContext = GlueContext(sc)
spark       = glueContext.spark_session
job         = Job(glueContext)
job.init(args["JOB_NAME"], args)

BUCKET     = args["S3_BUCKET"]
START_DATE = args["START_DATE"]
END_DATE   = args["END_DATE"]

print(f"🚀 Starting Glue ETL: {START_DATE} → {END_DATE}")

# ── Upgrade #2: Dynamic partition overwrite ───────────────────────────────────
# Only overwrites partitions being written in this run — safe for incremental loads.
spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")


# ── Upgrade #1: Incremental load — only read partitions in date range ─────────
def date_partition_paths(bucket: str, prefix: str, start: str, end: str) -> list:
    """
    Build explicit S3 partition paths for dates in [start, end].
    Avoids scanning the entire lake — Spark only reads relevant partitions.
    """
    paths   = []
    current = datetime.strptime(start, "%Y-%m-%d")
    end_dt  = datetime.strptime(end,   "%Y-%m-%d")

    while current <= end_dt:
        y = current.strftime("%Y")
        m = current.strftime("%m")
        d = current.strftime("%d")
        paths.append(f"s3://{bucket}/{prefix}/year={y}/month={m}/day={d}/")
        current += timedelta(days=1)

    return paths

weather_paths = date_partition_paths(BUCKET, "raw/weather", START_DATE, END_DATE)
energy_paths  = date_partition_paths(BUCKET, "raw/energy",  START_DATE, END_DATE)

print(f"📂 Reading {len(weather_paths)} weather partitions")
print(f"📂 Reading {len(energy_paths)} energy partitions")

weather_df = spark.read.option("multiline", "true").json(weather_paths)
energy_df  = spark.read.option("multiline", "true").json(energy_paths)

weather_raw_count = weather_df.count()
energy_raw_count  = energy_df.count()
print(f"✅ Weather records loaded: {weather_raw_count}")
print(f"✅ Energy records loaded:  {energy_raw_count}")


# ── Clean weather ─────────────────────────────────────────────────────────────
weather_clean = weather_df.select(
    F.col("city"),
    F.col("date"),
    F.col("temp_max_f").cast(DoubleType()),
    F.col("temp_min_f").cast(DoubleType()),
    F.col("temp_mean_f").cast(DoubleType()),
    F.col("precipitation_mm").cast(DoubleType()),
    F.col("windspeed_max_kmh").cast(DoubleType()),
).filter(F.col("temp_mean_f").isNotNull())

# ── Clean energy ──────────────────────────────────────────────────────────────
energy_clean = energy_df.select(
    F.col("city"),
    F.col("date"),
    F.col("region_id"),
    F.col("avg_demand_mwh").cast(DoubleType()),
    F.col("max_demand_mwh").cast(DoubleType()),
    F.col("min_demand_mwh").cast(DoubleType()),
    F.col("hours_collected").cast(IntegerType()),
).filter(F.col("avg_demand_mwh").isNotNull())

# ── Join on city + date ───────────────────────────────────────────────────────
joined_df = weather_clean.join(energy_clean, on=["city", "date"], how="inner")

# ── Add derived columns ───────────────────────────────────────────────────────
joined_df = joined_df.withColumn(
    "temp_range_f", F.round(F.col("temp_max_f") - F.col("temp_min_f"), 2)
).withColumn(
    "is_hot_day", F.when(F.col("temp_mean_f") >= 80, True).otherwise(False)
).withColumn(
    "is_cold_day", F.when(F.col("temp_mean_f") <= 32, True).otherwise(False)
).withColumn(
    "processed_at", F.lit(datetime.utcnow().isoformat())
).withColumn(
    "year",  F.year(F.to_date(F.col("date"))).cast("string")
).withColumn(
    "month", F.lpad(F.month(F.to_date(F.col("date"))).cast("string"), 2, "0")
).withColumn(
    "day",   F.lpad(F.dayofmonth(F.to_date(F.col("date"))).cast("string"), 2, "0")
)

joined_count = joined_df.count()
print(f"✅ Joined records: {joined_count}")


# ── Upgrade #5: Data quality checks ──────────────────────────────────────────

print("🔍 Running data quality checks...")
dq_passed = True
dq_warnings = []

# Check 1 — No duplicate (city, date) pairs
dupes = joined_df.groupBy("city", "date").count().filter(F.col("count") > 1)
dupe_count = dupes.count()
if dupe_count > 0:
    # Hard failure — duplicates corrupt downstream analytics
    dupes.show(10, truncate=False)
    raise Exception(
        f"❌ DQ FAILED: {dupe_count} duplicate (city, date) pairs found. "
        f"Aborting write to prevent corrupt curated layer."
    )
print(f"  ✅ Duplicates check passed (0 duplicate city+date pairs)")

# Check 2 — Energy completeness: hours_collected should be 24
incomplete = joined_df.filter(F.col("hours_collected") < 24)
incomplete_count = incomplete.count()
if incomplete_count > 0:
    # Soft warning — partial days are expected for yesterday's data near cutoff
    msg = f"⚠️  {incomplete_count} records have hours_collected < 24 (incomplete energy days)"
    print(f"  {msg}")
    dq_warnings.append(msg)
else:
    print(f"  ✅ Hours completeness check passed (all records have 24 hours)")

# Check 3 — Temperature plausibility (US cities, °F)
out_of_range = joined_df.filter(
    (F.col("temp_mean_f") < -60) | (F.col("temp_mean_f") > 140)
)
oor_count = out_of_range.count()
if oor_count > 0:
    out_of_range.select("city", "date", "temp_mean_f").show(10, truncate=False)
    raise Exception(
        f"❌ DQ FAILED: {oor_count} records with implausible temp_mean_f "
        f"(outside -60°F to 140°F). Possible API corruption."
    )
print(f"  ✅ Temperature plausibility check passed")

# Check 4 — Demand plausibility (MWh, regional grid)
bad_demand = joined_df.filter(
    (F.col("avg_demand_mwh") <= 0) | (F.col("avg_demand_mwh") > 500000)
)
bad_demand_count = bad_demand.count()
if bad_demand_count > 0:
    bad_demand.select("city", "date", "region_id", "avg_demand_mwh").show(10, truncate=False)
    raise Exception(
        f"❌ DQ FAILED: {bad_demand_count} records with implausible avg_demand_mwh "
        f"(must be > 0 and <= 500,000 MWh)."
    )
print(f"  ✅ Demand plausibility check passed")

# Check 5 — Expected record volume (50 cities per date)
date_counts = joined_df.groupBy("date").count()
thin_dates  = date_counts.filter(F.col("count") < 40)   # allow some tolerance
thin_count  = thin_dates.count()
if thin_count > 0:
    msg = f"⚠️  {thin_count} dates have fewer than 40 city records (expected 50)"
    print(f"  {msg}")
    thin_dates.orderBy("count").show(10, truncate=False)
    dq_warnings.append(msg)
else:
    print(f"  ✅ Record volume check passed (all dates have ≥ 40 city records)")

print(f"🔍 Data quality complete — {len(dq_warnings)} warning(s), 0 hard failures")
for w in dq_warnings:
    print(f"   {w}")


# ── Upgrade #2: Write with dynamic partition overwrite ────────────────────────
# Only partitions in this date range are overwritten — existing history untouched.
output_path = f"s3://{BUCKET}/curated/weather_energy/"

print(f"💾 Writing {joined_count} records to {output_path}")
print(f"   Mode: dynamic partition overwrite (only {len(weather_paths)} partitions affected)")

joined_df.write \
    .mode("overwrite") \
    .partitionBy("year", "month", "day") \
    .parquet(output_path)

print(f"✅ Written to {output_path}")

job.commit()
print(
    f"🎉 Glue ETL complete!\n"
    f"   Date range:     {START_DATE} → {END_DATE}\n"
    f"   Raw weather:    {weather_raw_count} records\n"
    f"   Raw energy:     {energy_raw_count} records\n"
    f"   Joined+written: {joined_count} records\n"
    f"   DQ warnings:    {len(dq_warnings)}"
)