import sys
import os
sys.path.insert(0, os.path.dirname(__file__))

import boto3
import json
from datetime import datetime, timedelta
from dotenv import load_dotenv
from fetch_weather import fetch_weather_range
from fetch_energy import fetch_energy
from upload_to_s3 import upload_weather, upload_energy

load_dotenv()

bucket = os.getenv("S3_BUCKET_NAME")
s3 = boto3.client("s3")


def date_exists(date_str: str) -> bool:
    year, month, day = date_str.split("-")
    key = f"raw/weather/year={year}/month={month}/day={day}/weather_{year}{month}{day}.json"
    try:
        s3.head_object(Bucket=bucket, Key=key)
        return True
    except:
        return False


def daterange(start: str, end: str):
    current = datetime.strptime(start, "%Y-%m-%d")
    end_dt  = datetime.strptime(end,   "%Y-%m-%d")
    while current <= end_dt:
        yield current.strftime("%Y-%m-%d")
        current += timedelta(days=1)


if __name__ == "__main__":
    START_DATE = "2023-01-01"
    END_DATE   = (datetime.utcnow() - timedelta(days=1)).strftime("%Y-%m-%d")

    all_dates    = list(daterange(START_DATE, END_DATE))
    pending      = [d for d in all_dates if not date_exists(d)]
    already_done = len(all_dates) - len(pending)

    print(f"📅 Total dates:  {len(all_dates)}")
    print(f"⏭️  Already done: {already_done}")
    print(f"🔄 Pending:      {len(pending)}\n")

    if not pending:
        print("✅ Already complete!")
        exit()

    # Fetch ALL weather in bulk — 50 cities × 1 call each (not 50 × 730!)
    print("🌤  Fetching ALL weather data in bulk...")
    weather_by_date = fetch_weather_range(START_DATE, END_DATE)
    print(f"✅ Weather done — {len(weather_by_date)} dates loaded\n")

    # Upload weather for pending dates only
    print("☁️  Uploading weather to S3...")
    for date_str in pending:
        if date_str in weather_by_date:
            upload_weather(weather_by_date[date_str], date_str)
    print("✅ Weather uploaded\n")

    # Fetch + upload energy date by date (EIA doesn't support bulk)
    print("⚡ Fetching energy data day by day...")
    success, failed = 0, 0
    for i, date_str in enumerate(pending):
        try:
            energy_data = fetch_energy(date_str)
            upload_energy(energy_data, date_str)
            success += 1
            print(f"[{i+1}/{len(pending)}] ✅ {date_str}")
        except Exception as e:
            failed += 1
            print(f"[{i+1}/{len(pending)}] ❌ {date_str} — {e}")

    print(f"\n{'='*50}")
    print(f"✅ Success: {success}")
    print(f"❌ Failed:  {failed}")