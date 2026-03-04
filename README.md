# Weather & Energy Demand Pipeline 🌤⚡

An end-to-end data engineering pipeline that ingests daily weather and electricity demand data for **50 US cities**, transforms it using Apache Spark on AWS Glue, and makes it queryable via Athena SQL — fully automated with a daily Lambda + EventBridge schedule.

## Pipeline Stats

| Metric | Value |
|--------|-------|
| Cities tracked | 50 |
| Date range | Jan 2023 → present (3+ years) |
| Total raw records | 107,550+ |
| Raw S3 partitions | 2,151 |
| Curated Parquet size | 6.14 MB (Snappy-compressed) |
| Curated partitions | 989 |
| Daily runs (confirmed) | 1,158 |
| Pipeline success rate | 95%+ |

---

## Architecture

```
┌─────────────────────┐     ┌─────────────────────┐
│   Open-Meteo API    │     │      EIA API         │
│  (Weather Data)     │     │  (Energy Demand)     │
└────────┬────────────┘     └──────────┬───────────┘
         │                             │
         └──────────┬──────────────────┘
                    ▼
         ┌─────────────────────┐
         │    AWS Lambda       │  ← Triggered daily at 6AM UTC
         │  (Python 3.11)      │    via EventBridge cron
         └────────┬────────────┘
                  │
                  ▼
         ┌─────────────────────┐
         │       AWS S3        │  ← Raw JSON, partitioned by
         │    (Data Lake)      │    year=/month=/day=
         └────────┬────────────┘
                  │
                  ▼
         ┌─────────────────────┐
         │     AWS Glue        │  ← PySpark ETL: cleans, joins,
         │   (ETL Job)         │    writes Parquet w/ Snappy
         └────────┬────────────┘
                  │
                  ▼
         ┌─────────────────────┐
         │       AWS S3        │  ← Curated Parquet, partitioned
         │  (Curated Layer)    │    by year=/month=/day=
         └────────┬────────────┘
                  │
                  ▼
         ┌─────────────────────┐
         │    AWS Athena        │  ← Serverless SQL on top of S3
         │  (SQL Queries)      │    via Glue Data Catalog
         └─────────────────────┘
```

---

## Data Sources

| Source | API | Data | Frequency |
|--------|-----|------|-----------|
| [Open-Meteo](https://open-meteo.com/) | Free, no key required | Max/min/mean temp, precipitation, windspeed | Daily |
| [EIA Open Data](https://www.eia.gov/opendata/) | Free, key required | Hourly electricity demand (MWh) by grid region | Daily |

### Cities Tracked (50 total)

| Grid Region | Cities |
|-------------|--------|
| NYIS | New York, Buffalo |
| ISNE | Philadelphia, Baltimore, Washington DC, Richmond, Virginia Beach, Raleigh |
| MISO | Chicago, Indianapolis, Columbus, Cleveland, Milwaukee, Minneapolis, Kansas City, St. Louis, Memphis, New Orleans, Omaha |
| PJM | Pittsburgh, Cincinnati, Louisville, Charlotte |
| ERCO | Houston, Dallas, San Antonio, Austin, Fort Worth, El Paso |
| AZPS | Phoenix, Tucson, Albuquerque, Las Vegas, Salt Lake City |
| CISO | Los Angeles, San Diego, San Jose, San Francisco, Fresno, Sacramento |
| PACW | Seattle, Portland, Boise |
| SWPP | Oklahoma City, Denver |
| SOCO/FPL/TEC/TVA | Atlanta, Miami, Tampa, Jacksonville, Nashville |

---

## Project Structure

```
weather-energy-pipeline/
├── src/
│   ├── ingestion/
│   │   ├── fetch_weather.py       # Pulls daily weather from Open-Meteo
│   │   ├── fetch_energy.py        # Pulls daily energy demand from EIA
│   │   ├── upload_to_s3.py        # Uploads raw JSON to S3 with partitioning
│   │   └── lambda_handler.py      # AWS Lambda entry point
│   ├── transformation/
│   │   └── glue_transform.py      # PySpark ETL: clean, join, write Parquet
│   └── sql/
│       └── analytics_queries.sql  # Athena analytical SQL queries
├── requirements.txt
├── .env                           # Local env vars (never committed)
├── .gitignore
└── README.md
```

---

## AWS Infrastructure

| Service | Purpose |
|---------|---------|
| **S3** | Data lake — raw JSON and curated Parquet storage |
| **Lambda** | Serverless ingestion function (Python 3.11, 256MB, 120s timeout) |
| **EventBridge** | Daily cron trigger at 6:00 AM UTC |
| **Glue** | Managed PySpark ETL — cleans, joins, and transforms data |
| **Glue Data Catalog** | Metadata store — makes S3 data queryable by Athena |
| **Athena** | Serverless SQL engine on top of S3/Parquet |
| **IAM** | Least-privilege roles for Lambda and Glue |

### S3 Bucket Layout

```
weather-energy-pipeline-dchau/
├── raw/                                          # 2,151 partitions
│   ├── weather/year=2023/month=01/day=01/weather_20230101.json
│   └── energy/year=2023/month=01/day=01/energy_20230101.json
├── curated/                                      # 989 partitions, 6.14 MB
│   └── weather_energy/year=2023/month=01/day=01/*.snappy.parquet
└── scripts/
    └── glue_transform.py
```

---

## Data Pipeline Stages

### Stage 1 — Ingestion (Lambda)
- Fetches previous day's weather data for 50 cities from Open-Meteo archive API
- Fetches 24 hourly electricity demand readings per city from EIA API (50 grid regions)
- Uploads raw JSON to S3 with `year=/month=/day=` partitioning — 100 files per day
- Runs automatically every day at 6AM UTC via EventBridge
- Confirmed 1,158+ successful daily runs with 95%+ success rate

### Stage 2 — Transformation (Glue / PySpark)
- Reads 107,550+ raw records from S3 across 2,151 partitions with multiline JSON parsing
- Casts all fields to correct types (DoubleType, IntegerType)
- Filters out null records for data quality
- Joins weather and energy datasets on `city` + `date`
- Engineers 3 derived features:
  - `temp_range_f` — daily temperature swing
  - `is_hot_day` — boolean flag for days ≥ 80°F
  - `is_cold_day` — boolean flag for days ≤ 32°F
- Writes 6.14 MB of Snappy-compressed Parquet across 989 date partitions

### Stage 3 — Analytics (Athena)
- Serverless SQL queries over Parquet files in S3
- Partition pruning keeps query costs minimal
- Glue Data Catalog provides schema management

---

## Sample SQL Queries

```sql
-- Cities ranked by energy demand
SELECT city, date, temp_mean_f, avg_demand_mwh
FROM weather_energy_db.weather_energy_joined
ORDER BY avg_demand_mwh DESC;

-- Temperature categories and their energy impact
SELECT
    city,
    date,
    temp_mean_f,
    avg_demand_mwh,
    CASE
        WHEN temp_mean_f < 32 THEN 'Freezing'
        WHEN temp_mean_f < 50 THEN 'Cold'
        WHEN temp_mean_f < 70 THEN 'Mild'
        ELSE 'Hot'
    END AS temp_category
FROM weather_energy_db.weather_energy_joined
ORDER BY temp_mean_f ASC;

-- Average demand vs temperature per city (correlation analysis)
SELECT
    city,
    AVG(temp_mean_f)    AS avg_temp,
    AVG(avg_demand_mwh) AS avg_demand,
    MAX(avg_demand_mwh) AS peak_demand,
    MIN(temp_mean_f)    AS coldest_day
FROM weather_energy_db.weather_energy_joined
GROUP BY city
ORDER BY avg_demand DESC;
```

---

## Local Setup

### Prerequisites
- Python 3.10+
- AWS CLI configured (`aws configure`)
- EIA API key from [eia.gov/opendata](https://www.eia.gov/opendata/)

### Installation

```bash
git clone https://github.com/yourusername/weather-energy-pipeline
cd weather-energy-pipeline

python -m venv venv
source venv/bin/activate  # Windows: venv\Scripts\activate

pip install -r requirements.txt
```

### Environment Variables

Create a `.env` file in the project root:

```
EIA_API_KEY=your_eia_api_key_here
S3_BUCKET_NAME=weather-energy-pipeline-yourname
```

### Run Locally

```bash
# Test ingestion scripts
python src/ingestion/fetch_weather.py
python src/ingestion/fetch_energy.py

# Fetch and upload to S3
cd src/ingestion
python upload_to_s3.py
```

---

## Deployment

### Deploy Lambda

```bash
# Package dependencies
pip install requests python-dotenv boto3 --target src/ingestion/package
cd src/ingestion/package && zip -r ../../../lambda_deployment.zip . && cd ../../..
zip -j lambda_deployment.zip src/ingestion/*.py

# Deploy
aws lambda create-function \
  --function-name weather-energy-ingestion \
  --runtime python3.11 \
  --role arn:aws:iam::YOUR_ACCOUNT_ID:role/weather-energy-lambda-role \
  --handler lambda_handler.handler \
  --zip-file fileb://lambda_deployment.zip \
  --timeout 120 --memory-size 256
```

### Update Lambda After Code Changes

```bash
zip -j lambda_deployment.zip src/ingestion/*.py
aws lambda update-function-code \
  --function-name weather-energy-ingestion \
  --zip-file fileb://lambda_deployment.zip
```

### Trigger Glue Job Manually

```bash
aws glue start-job-run \
  --job-name weather-energy-etl \
  --arguments '{"--START_DATE": "2023-01-01", "--END_DATE": "2026-03-02", "--S3_BUCKET": "weather-energy-pipeline-yourname"}'
```

---

## Known Limitations

- IAM roles use broad managed policies (e.g. `AmazonS3FullAccess`) — in production, these would be scoped to specific bucket ARNs using least-privilege custom policies
- EIA data has a ~1-2 day lag, so the pipeline fetches the previous day's data
- Energy demand figures represent regional grid totals (not city-level) due to EIA API structure
- Python 3.9 is approaching end-of-support for boto3 (April 2026) — upgrading to 3.11+ is recommended
- Open-Meteo free tier rate limits bulk historical backfills — the backfill script uses exponential backoff and bulk date-range fetching (one API call per city for the full range) to work within limits
- 31 out of 1,158 energy dates failed during backfill due to EIA gateway timeouts — these can be retried safely as the backfill script is idempotent

---

## Tech Stack

![Python](https://img.shields.io/badge/Python-3.11-blue)
![AWS](https://img.shields.io/badge/AWS-Lambda%20%7C%20S3%20%7C%20Glue%20%7C%20Athena-orange)
![Apache Spark](https://img.shields.io/badge/Apache-Spark%20%28PySpark%29-red)
![SQL](https://img.shields.io/badge/SQL-Athena-lightgrey)