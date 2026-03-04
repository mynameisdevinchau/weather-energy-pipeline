import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.context import SparkContext
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, IntegerType
from datetime import datetime

# --- Initialize Glue + Spark context ---
args = getResolvedOptions(sys.argv, ["JOB_NAME", "S3_BUCKET", "START_DATE", "END_DATE"])

sc          = SparkContext()
glueContext = GlueContext(sc)
spark       = glueContext.spark_session
job         = Job(glueContext)
job.init(args["JOB_NAME"], args)

BUCKET     = args["S3_BUCKET"]
START_DATE = args["START_DATE"]
END_DATE   = args["END_DATE"]

print(f"🚀 Starting Glue ETL from {START_DATE} to {END_DATE}")

# --- Read ALL raw weather and energy at once ---
weather_path = f"s3://{BUCKET}/raw/weather/"
energy_path  = f"s3://{BUCKET}/raw/energy/"

weather_df = spark.read.option("multiline", "true").json(weather_path)
energy_df  = spark.read.option("multiline", "true").json(energy_path)

print(f"✅ Weather records loaded: {weather_df.count()}")
print(f"✅ Energy records loaded:  {energy_df.count()}")

# --- Clean weather ---
weather_clean = weather_df.select(
    F.col("city"),
    F.col("date"),
    F.col("temp_max_f").cast(DoubleType()),
    F.col("temp_min_f").cast(DoubleType()),
    F.col("temp_mean_f").cast(DoubleType()),
    F.col("precipitation_mm").cast(DoubleType()),
    F.col("windspeed_max_kmh").cast(DoubleType()),
).filter(F.col("temp_mean_f").isNotNull())

# --- Clean energy ---
energy_clean = energy_df.select(
    F.col("city"),
    F.col("date"),
    F.col("region_id"),
    F.col("avg_demand_mwh").cast(DoubleType()),
    F.col("max_demand_mwh").cast(DoubleType()),
    F.col("min_demand_mwh").cast(DoubleType()),
    F.col("hours_collected").cast(IntegerType()),
).filter(F.col("avg_demand_mwh").isNotNull())

# --- Join on city + date ---
joined_df = weather_clean.join(energy_clean, on=["city", "date"], how="inner")

# --- Add derived columns ---
joined_df = joined_df.withColumn(
    "temp_range_f", F.round(F.col("temp_max_f") - F.col("temp_min_f"), 2)
).withColumn(
    "is_hot_day", F.when(F.col("temp_mean_f") >= 80, True).otherwise(False)
).withColumn(
    "is_cold_day", F.when(F.col("temp_mean_f") <= 32, True).otherwise(False)
).withColumn(
    "processed_at", F.lit(datetime.utcnow().isoformat())
).withColumn("year",  F.year(F.to_date(F.col("date"))).cast("string")
).withColumn("month", F.lpad(F.month(F.to_date(F.col("date"))).cast("string"), 2, "0")
).withColumn("day",   F.lpad(F.dayofmonth(F.to_date(F.col("date"))).cast("string"), 2, "0"))

print(f"✅ Joined records: {joined_df.count()}")

# --- Write to curated S3 as Parquet, partitioned by date ---
output_path = f"s3://{BUCKET}/curated/weather_energy/"

joined_df.write \
    .mode("overwrite") \
    .partitionBy("year", "month", "day") \
    .parquet(output_path)

print(f"✅ Written to {output_path}")

job.commit()
print("🎉 Glue ETL job complete!")