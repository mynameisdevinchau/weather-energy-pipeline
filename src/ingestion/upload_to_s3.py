import boto3
import json
import os
from datetime import datetime
from dotenv import load_dotenv

load_dotenv()

BUCKET_NAME = os.getenv("S3_BUCKET_NAME")
AWS_REGION  = os.getenv("AWS_REGION", "us-east-1")

s3_client = boto3.client("s3", region_name=AWS_REGION)


def upload_json(data: list[dict], dataset: str, date_str: str) -> str:
    """
    Upload a list of records to S3 as JSON.
    
    Partitioned path format:
      raw/{dataset}/year=YYYY/month=MM/day=DD/{dataset}_YYYYMMDD.json
    
    This date-partitioned structure is a standard data lake pattern
    and makes Athena queries much faster (partition pruning).
    """
    year, month, day = date_str.split("-")

    s3_key = f"raw/{dataset}/year={year}/month={month}/day={day}/{dataset}_{year}{month}{day}.json"

    payload = json.dumps(data, indent=2)

    s3_client.put_object(
        Bucket=BUCKET_NAME,
        Key=s3_key,
        Body=payload,
        ContentType="application/json",
    )

    s3_uri = f"s3://{BUCKET_NAME}/{s3_key}"
    print(f"✅ Uploaded {len(data)} records to {s3_uri}")
    return s3_uri


def upload_weather(weather_data: list[dict], date_str: str) -> str:
    return upload_json(weather_data, "weather", date_str)


def upload_energy(energy_data: list[dict], date_str: str) -> str:
    return upload_json(energy_data, "energy", date_str)


if __name__ == "__main__":
    # Test: fetch and upload both datasets
    from fetch_weather import fetch_weather
    from fetch_energy  import fetch_energy

    from datetime import timedelta
    date_str = (datetime.utcnow() - timedelta(days=1)).strftime("%Y-%m-%d")

    print(f"\n📅 Fetching data for {date_str}...\n")

    weather_data = fetch_weather(date_str)
    upload_weather(weather_data, date_str)

    energy_data = fetch_energy(date_str)
    upload_energy(energy_data, date_str)

    print("\n🎉 All data uploaded to S3!")