# Weather & Energy Demand Pipeline 🌤⚡

![Python](https://img.shields.io/badge/Python-3.11-blue)
![AWS](https://img.shields.io/badge/AWS-Lambda%20%7C%20S3%20%7C%20Glue%20%7C%20Athena%20%7C%20DynamoDB-orange)
![Apache Spark](https://img.shields.io/badge/Apache-PySpark-red)
![SQL](https://img.shields.io/badge/SQL-Athena-lightgrey)
![Status](https://img.shields.io/badge/pipeline-live-brightgreen)

---

## Motivation

Electricity demand in the US is not constant — it swings dramatically with the weather. On the hottest summer days, air conditioning can push grid demand to dangerous peaks. During cold snaps, heating loads can strain regional operators to their limits. Yet the relationship between temperature and demand varies significantly by region: a 95°F day in Phoenix looks very different on the grid than the same temperature in Seattle.

Understanding this relationship at scale — across cities, seasons, and years — requires joining two datasets that are rarely combined: granular daily weather observations and hourly electricity demand by grid region. This pipeline was built to make that analysis possible.

**The core question this project answers:** *How does temperature drive electricity demand across different US cities and grid regions, and how do extreme weather events stress the grid compared to seasonal norms?*

To answer it reliably, the data needs to arrive every day, be trustworthy, and be structured for fast analytical queries. That's what this pipeline delivers.

---

## What This Enables

With 3+ years of joined weather and energy data across 50 cities, this dataset supports analysis that isn't possible with either source alone:

- **Seasonal demand baselines** — quantify exactly how much more electricity Chicago consumes in January vs July, and how that compares to Miami's inverse summer peak driven by air conditioning
- **Extreme weather and grid stress** — identify days where demand spiked far above seasonal norms and correlate them to temperature outliers, cold snaps, or heat waves
- **Regional sensitivity analysis** — compare how a 10°F temperature swing affects demand in the ERCO (Texas) grid vs the MISO (Midwest) grid, surfacing which regions are most weather-dependent
- **Year-over-year trends** — track whether demand for heating or cooling has shifted across the 2023–2026 window, a proxy for changes in building efficiency or broader climate patterns

---

## Pipeline Stats

| Metric | Value |
|--------|-------|
| Cities tracked | 50 across 13 US grid regions |
| Date range | Jan 2023 → present (3+ years of history) |
| Total raw records | 107,550+ |
| Raw S3 partitions | 2,151 |
| Curated Parquet size | 6.14 MB (Snappy-compressed) |
| Curated partitions | 989 |
| Confirmed daily runs | 1,158+ |
| Pipeline success rate | 95%+ (logged in DynamoDB per run) |
| Lambda runtime | ~100s (300s limit) |
| EIA API calls per run | 13 unique regions (not 50 sequential) |

---

## Architecture

```
┌─────────────────────┐          ┌──────────────────────┐
│   Open-Meteo API    │          │       EIA API         │
│   (Weather Data)    │          │   (Energy Demand)     │
│   archive-api       │          │   v2/electricity/rto  │
└────────┬────────────┘          └──────────┬────────────┘
         │  1 call/city/range               │  13 parallel region calls
         │  (bulk date range)               │  (ThreadPoolExecutor, 4 workers)
         └──────────────┬───────────────────┘
                        ▼
           ┌─────────────────────────┐
           │       AWS Lambda        │  ← EventBridge cron: daily 6AM UTC
           │     (Python 3.11)       │    Timeout: 300s | Memory: 256MB
           │                         │    On failure: raises → EventBridge
           │  • fetch weather         │    retries → SNS alert email
           │  • fetch energy          │
           │  • upload to S3          │
           │  • log run to DynamoDB   │
           │  • emit CloudWatch       │
           └────────────┬────────────┘
                        │
           ┌────────────┴────────────┐
           ▼                         ▼
┌─────────────────────┐   ┌─────────────────────┐
│  S3 Raw Layer       │   │  DynamoDB           │
│  (JSON, partitioned │   │  pipeline_runs      │
│  by year/month/day) │   │  (run metadata,     │
│                     │   │   status, counts,   │
│  raw/weather/...    │   │   duration, errors) │
│  raw/energy/...     │   └─────────────────────┘
└────────┬────────────┘
         │
         ▼
┌─────────────────────────────────────────────────┐
│               AWS Glue (PySpark ETL)            │
│                                                 │
│  1. Incremental load — partition path filter    │
│     (reads only START_DATE → END_DATE)          │
│  2. Schema casting + null filtering             │
│  3. Inner join on city + date                   │
│  4. Feature engineering (3 derived columns)     │
│  5. Data quality checks (5 validators)          │
│  6. Dynamic partition overwrite → Parquet       │
└────────────────────┬────────────────────────────┘
                     │
                     ▼
        ┌─────────────────────────┐
        │   S3 Curated Layer      │
        │   Snappy Parquet        │
        │   partitioned by        │
        │   year / month / day    │
        └────────────┬────────────┘
                     │
                     ▼
        ┌─────────────────────────┐
        │       AWS Athena        │  ← Serverless SQL
        │   (Glue Data Catalog)   │    Partition pruning
        └─────────────────────────┘
```

---

## Production Engineering Highlights

### Observability — DynamoDB + CloudWatch

Every Lambda invocation writes a structured record to a DynamoDB `pipeline_runs` table:

| Field | Type | Description |
|-------|------|-------------|
| `run_date` | String (PK) | Date ingested (YYYY-MM-DD) |
| `run_ts` | String | UTC timestamp of execution |
| `status` | String | `SUCCESS`, `PARTIAL`, or `FAILED` |
| `weather_records` | Number | Records fetched from Open-Meteo |
| `energy_records` | Number | Records fetched from EIA |
| `total_records` | Number | Combined record count |
| `duration_ms` | Number | End-to-end Lambda runtime |
| `error` | String | Error message if applicable |

Five custom CloudWatch metrics are emitted per run: `WeatherRecordsIngested`, `EnergyRecordsIngested`, `TotalRecordsIngested`, `IngestionDurationMs`, and `PipelineSuccess`.

### Alerting — SNS Email Notifications

The Lambda failure contract distinguishes two scenarios:

- **Partial failure** (one source down): logs `PARTIAL` to DynamoDB, sends a warning email via SNS, returns HTTP 200 so EventBridge does not retry and double-upload the successful source
- **Total failure** (both sources down): raises an exception so Lambda marks the invocation `FAILED` → EventBridge retries up to 2× → SNS alert email fires on each attempt

### Incremental Glue ETL

The Glue job accepts `START_DATE` and `END_DATE` arguments and builds explicit S3 partition paths for only those dates — Spark never scans outside the requested range. Combined with `spark.sql.sources.partitionOverwriteMode = dynamic`, only the partitions being written in a given run are overwritten; all historical Parquet data is untouched.

### API Efficiency — 13 Calls Instead of 50

Multiple cities share the same EIA grid region (e.g. Chicago, Indianapolis, Columbus, and 8 other cities all map to MISO). Rather than making 50 sequential API calls, the ingestion layer deduplicates to 13 unique region calls executed in parallel via `ThreadPoolExecutor(max_workers=4)`, then fans each result out to all cities in that region. This reduced Lambda runtime from >300s (timeout) to ~100s.

### Data Quality — 5 Validators in Glue

Before any data reaches the curated layer, the Glue job runs five checks:

| Check | Type | Behavior on Failure |
|-------|------|---------------------|
| Duplicate `(city, date)` pairs | Hard | Raises exception, aborts write |
| `hours_collected < 24` | Soft | Logs warning, continues |
| `temp_mean_f` outside −60°F to 140°F | Hard | Raises exception, aborts write |
| `avg_demand_mwh` outside 0 to 500,000 MWh | Hard | Raises exception, aborts write |
| Dates with fewer than 40 city records | Soft | Logs warning with detail, continues |

---

## Data Sources

| Source | API | Data | Update Lag |
|--------|-----|------|------------|
| [Open-Meteo](https://open-meteo.com/) | Free, no key required | Max/min/mean temp (°F), precipitation (mm), windspeed (km/h) | ~1 day |
| [EIA Open Data](https://www.eia.gov/opendata/) | Free, key required | Hourly electricity demand (MWh) by grid region | ~1–2 days |

### Cities Tracked (50 total, 13 grid regions)

| Grid Region | Region Description | Cities |
|-------------|--------------------|--------|
| NYIS | New York Independent System | New York, Buffalo |
| ISNE | ISO New England | Philadelphia, Baltimore, Washington DC, Richmond, Virginia Beach, Raleigh |
| MISO | Midcontinent ISO | Chicago, Indianapolis, Columbus, Cleveland, Milwaukee, Minneapolis, Kansas City, St. Louis, Memphis, New Orleans, Omaha |
| PJM | PJM Interconnection | Pittsburgh, Cincinnati, Louisville, Charlotte |
| ERCO | Electric Reliability Council of Texas | Houston, Dallas, San Antonio, Austin, Fort Worth, El Paso |
| AZPS | Arizona Public Service | Phoenix, Tucson, Albuquerque, Las Vegas, Salt Lake City |
| CISO | California ISO | Los Angeles, San Diego, San Jose, San Francisco, Fresno, Sacramento |
| PACW | Pacific West | Seattle, Portland, Boise |
| SWPP | Southwest Power Pool | Oklahoma City, Denver |
| SOCO | Southern Company | Atlanta |
| FPL | Florida Power & Light | Miami, Jacksonville |
| TEC | Tampa Electric | Tampa |
| TVA | Tennessee Valley Authority | Nashville |

---

## Data Schema

### Raw Layer — `raw/weather/*.json`

| Field | Type | Description |
|-------|------|-------------|
| `city` | string | City name |
| `date` | string | YYYY-MM-DD |
| `temp_max_f` | float | Daily high temperature (°F) |
| `temp_min_f` | float | Daily low temperature (°F) |
| `temp_mean_f` | float | Daily mean temperature (°F) |
| `precipitation_mm` | float | Total precipitation (mm) |
| `windspeed_max_kmh` | float | Max windspeed (km/h) |
| `ingested_at` | string | UTC ingestion timestamp |

### Raw Layer — `raw/energy/*.json`

| Field | Type | Description |
|-------|------|-------------|
| `city` | string | City name |
| `region_id` | string | EIA grid region code |
| `date` | string | YYYY-MM-DD |
| `avg_demand_mwh` | float | Mean hourly demand (MWh) |
| `max_demand_mwh` | float | Peak hourly demand (MWh) |
| `min_demand_mwh` | float | Minimum hourly demand (MWh) |
| `hours_collected` | int | Number of hours with data (target: 24) |
| `ingested_at` | string | UTC ingestion timestamp |

### Curated Layer — `curated/weather_energy/*.parquet`

All raw fields above, plus three engineered features:

| Field | Type | Description |
|-------|------|-------------|
| `temp_range_f` | float | `temp_max_f − temp_min_f` — daily temperature swing |
| `is_hot_day` | boolean | `true` if `temp_mean_f ≥ 80°F` |
| `is_cold_day` | boolean | `true` if `temp_mean_f ≤ 32°F` |
| `processed_at` | string | UTC Glue job execution timestamp |
| `year` | string | Partition key |
| `month` | string | Partition key (zero-padded) |
| `day` | string | Partition key (zero-padded) |

---

## AWS Infrastructure

| Service | Configuration | Purpose |
|---------|---------------|---------|
| **S3** | Standard storage class | Raw JSON data lake + curated Parquet + Glue scripts |
| **Lambda** | Python 3.11, 256MB, 300s timeout | Daily ingestion — weather + energy fetch + S3 upload |
| **EventBridge** | `cron(0 6 * * ? *)` | Triggers Lambda daily at 6:00 AM UTC |
| **Glue** | Version 3.0, 2× G.1X workers | Incremental PySpark ETL + data quality enforcement |
| **Glue Data Catalog** | `weather_energy_db` database | Schema registry — makes S3 queryable by Athena |
| **Athena** | Dedicated results bucket | Serverless SQL over Parquet with partition pruning |
| **DynamoDB** | `pipeline_runs`, PAY_PER_REQUEST | Per-run observability — status, counts, duration, errors |
| **CloudWatch** | Custom namespace `WeatherEnergyPipeline` | 5 metrics per run — ingestion volume, latency, success flag |
| **SNS** | Email subscription | Alerts on partial and total pipeline failures |
| **IAM** | Separate roles for Lambda + Glue | Scoped access to S3, DynamoDB, CloudWatch, SNS |

### S3 Bucket Layout

```
weather-energy-pipeline-dchau/
│
├── raw/                                              # 2,151 partitions
│   ├── weather/
│   │   └── year=2023/month=01/day=01/
│   │       └── weather_20230101.json                # 50 city records per file
│   └── energy/
│       └── year=2023/month=01/day=01/
│           └── energy_20230101.json                 # 50 city records per file
│
├── curated/                                         # 989 partitions, 6.14 MB total
│   └── weather_energy/
│       └── year=2023/month=01/day=01/
│           └── part-00000-*.snappy.parquet
│
└── scripts/
    └── glue_transform.py
```

---

## Project Structure

```
weather-energy-pipeline/
├── src/
│   ├── ingestion/
│   │   ├── fetch_weather.py       # Open-Meteo bulk range fetch (1 call/city)
│   │   ├── fetch_energy.py        # EIA parallel fetch (13 regions, 4 workers)
│   │   ├── upload_to_s3.py        # Date-partitioned S3 upload
│   │   ├── lambda_handler.py      # Lambda entry point + DynamoDB/CloudWatch/SNS
│   │   ├── backfill_bulk.py       # Historical backfill (resumable, skip-existing)
│   │   └── setup_infra.py         # One-time DynamoDB + SNS provisioning script
│   ├── transformation/
│   │   └── glue_transform.py      # Incremental PySpark ETL + 5 DQ checks
│   └── sql/
│       └── analytics_queries.sql  # Athena analytical queries
├── requirements.txt
├── .env                           # Local env vars (never committed)
├── .gitignore
└── README.md
```

---

## Data Pipeline Stages

### Stage 1 — Ingestion (Lambda, ~100s runtime)

Weather and energy are fetched and uploaded to S3 with Hive-style partitioning. Each successful run appends 100 new files (50 weather + 50 energy) to the raw layer.

**Weather fetch** (`fetch_weather.py`): calls Open-Meteo's archive API once per city for the target date. Each call returns all 5 daily metrics in a single response. Retries up to 6× with exponential backoff on 429s; raises loudly on total failure so the Lambda handler can catch and log it properly.

**Energy fetch** (`fetch_energy.py`): deduplicates 50 cities into 13 unique EIA grid regions, fetches all 13 in parallel using `ThreadPoolExecutor(max_workers=4)`, then fans each region's hourly demand stats out to all cities in that region. This reduces API calls by 74% and cuts runtime from >300s to ~100s.

**Observability** (`lambda_handler.py`): after every run — success or failure — writes a structured record to DynamoDB and emits 5 CloudWatch metrics. Partial failure sends an SNS warning and returns 200. Total failure raises an exception so EventBridge retries and SNS fires an alert email.

### Stage 2 — Transformation (Glue PySpark)

The Glue job is designed for incremental daily runs. It accepts `START_DATE` and `END_DATE`, builds explicit S3 partition paths for only those dates, and uses `partitionOverwriteMode=dynamic` — only newly processed partitions are written, all historical Parquet is untouched.

**Data quality** runs before writing: duplicate detection on `(city, date)`, energy completeness check, temperature plausibility bounds, demand plausibility bounds, and per-date record volume. Hard failures abort the write; soft warnings log detail and continue.

**Feature engineering** produces `temp_range_f`, `is_hot_day`, and `is_cold_day` to support temperature-segmented energy demand analysis downstream.

### Stage 3 — Analytics (Athena)

Athena queries the curated Parquet layer through the Glue Data Catalog. Columnar Parquet with Snappy compression and `year/month/day` partitioning means date-filtered queries scan only the necessary files — keeping query costs minimal on this dataset size.

---

## Sample SQL Queries

```sql
-- Temperature vs energy demand correlation by city (3-year view)
SELECT
    city,
    ROUND(AVG(temp_mean_f), 1)    AS avg_temp_f,
    ROUND(AVG(avg_demand_mwh), 0) AS avg_demand_mwh,
    ROUND(MAX(avg_demand_mwh), 0) AS peak_demand_mwh,
    ROUND(MIN(temp_mean_f), 1)    AS coldest_day_f
FROM weather_energy_db.weather_energy_joined
GROUP BY city
ORDER BY avg_demand_mwh DESC;

-- Seasonal energy demand patterns
SELECT
    city,
    CASE
        WHEN CAST(month AS INTEGER) IN (12, 1, 2) THEN 'Winter'
        WHEN CAST(month AS INTEGER) IN (3, 4, 5)  THEN 'Spring'
        WHEN CAST(month AS INTEGER) IN (6, 7, 8)  THEN 'Summer'
        ELSE 'Fall'
    END                               AS season,
    ROUND(AVG(avg_demand_mwh), 0) AS avg_demand_mwh,
    ROUND(AVG(temp_mean_f), 1)    AS avg_temp_f
FROM weather_energy_db.weather_energy_joined
GROUP BY city, 2
ORDER BY city, avg_demand_mwh DESC;

-- Extreme weather days and their energy impact
SELECT
    city,
    date,
    temp_mean_f,
    temp_range_f,
    avg_demand_mwh,
    CASE
        WHEN is_hot_day  = true THEN 'Hot (≥80°F)'
        WHEN is_cold_day = true THEN 'Cold (≤32°F)'
        ELSE 'Moderate'
    END AS temp_category
FROM weather_energy_db.weather_energy_joined
WHERE is_hot_day = true OR is_cold_day = true
ORDER BY avg_demand_mwh DESC
LIMIT 100;

-- Year-over-year demand trends per city
SELECT
    city,
    year,
    ROUND(AVG(avg_demand_mwh), 0) AS avg_annual_demand_mwh,
    COUNT(*)                       AS days_with_data
FROM weather_energy_db.weather_energy_joined
GROUP BY city, year
ORDER BY city, year;
```

---

## Local Setup

### Prerequisites
- Python 3.10+
- AWS CLI configured (`aws configure`)
- EIA API key — register free at [eia.gov/opendata](https://www.eia.gov/opendata/)

### Installation

```bash
git clone https://github.com/yourusername/weather-energy-pipeline
cd weather-energy-pipeline

python -m venv venv
source venv/bin/activate   # Windows: venv\Scripts\activate

pip install -r requirements.txt
```

### Environment Variables

Create a `.env` file in the project root:

```
EIA_API_KEY=your_eia_api_key_here
S3_BUCKET_NAME=weather-energy-pipeline-yourname
```

Lambda reads these from its own environment variables configured separately — never from `.env`.

### Run Locally

```bash
# Test a single day end-to-end
python src/ingestion/fetch_weather.py
python src/ingestion/fetch_energy.py

# Run historical backfill (resumable — skips dates already in S3)
cd src/ingestion
python backfill_bulk.py
```

---

## Deployment

### 1. Provision Observability Infrastructure (run once)

```bash
cd src/ingestion
python setup_infra.py
# Creates: DynamoDB table, SNS topic + email subscription, IAM inline policy
# Prints: exact Lambda env var update command with your SNS ARN pre-filled
```

### 2. Deploy Lambda

```bash
# Package dependencies
pip install requests python-dotenv boto3 --target src/ingestion/package
cd src/ingestion/package && zip -r ../../../lambda_deployment.zip . && cd ../../..
zip -j lambda_deployment.zip src/ingestion/*.py

# Create function
aws lambda create-function \
  --function-name weather-energy-ingestion \
  --runtime python3.11 \
  --role arn:aws:iam::YOUR_ACCOUNT_ID:role/weather-energy-lambda-role \
  --handler lambda_handler.handler \
  --zip-file fileb://lambda_deployment.zip \
  --timeout 300 \
  --memory-size 256 \
  --environment 'Variables={
    "EIA_API_KEY":    "your_key",
    "S3_BUCKET_NAME": "weather-energy-pipeline-yourname",
    "PIPELINE_TABLE": "pipeline_runs",
    "SNS_ALERT_ARN":  "arn:aws:sns:us-east-1:ACCOUNT:weather-energy-pipeline-alerts"
  }'
```

### 3. Update Lambda After Code Changes

```bash
zip -j lambda_deployment.zip src/ingestion/*.py
aws lambda update-function-code \
  --function-name weather-energy-ingestion \
  --zip-file fileb://lambda_deployment.zip
```

### 4. Upload Glue Script

```bash
aws s3 cp src/transformation/glue_transform.py \
  s3://weather-energy-pipeline-yourname/scripts/glue_transform.py
```

### 5. Run Glue Job

```bash
# Daily incremental run (single day)
aws glue start-job-run \
  --job-name weather-energy-etl \
  --arguments '{
    "--START_DATE": "2026-03-11",
    "--END_DATE":   "2026-03-11",
    "--S3_BUCKET":  "weather-energy-pipeline-yourname"
  }'

# Full historical reprocess
aws glue start-job-run \
  --job-name weather-energy-etl \
  --arguments '{
    "--START_DATE": "2023-01-01",
    "--END_DATE":   "2026-03-11",
    "--S3_BUCKET":  "weather-energy-pipeline-yourname"
  }'
```

### 6. Set Up CloudWatch Alarm

In the AWS Console → CloudWatch → Alarms → Create Alarm:
- Metric: `WeatherEnergyPipeline / PipelineSuccess`
- Condition: `< 1` for `1 datapoint within 1 day`
- Action: send to `weather-energy-pipeline-alerts` SNS topic

This triggers an alert email any day the pipeline does not complete successfully.

---

## Checking Pipeline Health

```bash
# View a specific run log
aws dynamodb get-item \
  --table-name pipeline_runs \
  --key '{"run_date": {"S": "2026-03-11"}}'

# Scan all failed runs
aws dynamodb scan \
  --table-name pipeline_runs \
  --filter-expression "#s = :v" \
  --expression-attribute-names '{"#s": "status"}' \
  --expression-attribute-values '{":v": {"S": "FAILED"}}'

# Recent Lambda logs
aws logs tail /aws/lambda/weather-energy-ingestion --since 24h

# Count raw files in S3
aws s3 ls s3://weather-energy-pipeline-yourname/raw/weather/ --recursive | wc -l
aws s3 ls s3://weather-energy-pipeline-yourname/raw/energy/  --recursive | wc -l
```

---

## Known Limitations

- **IAM scope**: roles currently use broad managed policies (`AmazonS3FullAccess`) — in production these would be scoped to specific bucket ARNs and actions via least-privilege custom policies
- **EIA data lag**: EIA publishes data with a ~1–2 day lag; the pipeline always fetches the previous day to ensure completeness
- **Regional granularity**: energy demand figures represent entire grid region totals, not individual city consumption — all cities within a region share identical demand values by design, reflecting the EIA API's regional structure
- **Backfill gaps**: 31 of 1,158 energy dates failed during initial backfill due to EIA 504 gateway timeouts; the backfill script is idempotent and can be re-run to fill these without duplicating successful dates
- **Open-Meteo rate limits**: bulk historical backfill uses one API call per city for the full date range (not per-day calls) to stay within free tier rate limits; exponential backoff handles transient 429s

---

## Tech Stack

| Layer | Technology |
|-------|-----------|
| Language | Python 3.11 |
| Ingestion | AWS Lambda, EventBridge, Requests, ThreadPoolExecutor |
| Storage | AWS S3 (JSON + Parquet), Hive-style partitioning |
| Transformation | AWS Glue 3.0, PySpark, Snappy compression |
| Analytics | AWS Athena, Glue Data Catalog |
| Observability | DynamoDB, CloudWatch custom metrics, SNS email alerts |
| Infrastructure | AWS IAM, AWS CLI |
| APIs | Open-Meteo Archive API, EIA Open Data v2 |