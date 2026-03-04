import sys
import os
sys.path.insert(0, os.path.dirname(__file__))

from datetime import datetime, timedelta
from fetch_weather import fetch_weather
from fetch_energy import fetch_energy
from upload_to_s3 import upload_weather, upload_energy
from concurrent.futures import ThreadPoolExecutor, as_completed
import boto3
from dotenv import load_dotenv
import time

load_dotenv()

bucket = os.getenv("S3_BUCKET_NAME")
s3 = boto3.client("s3")


def date_exists_in_s3(date_str: str) -> bool:
    year, month, day = date_str.split("-")
    key = f"raw/weather/year={year}/month={month}/day={day}/weather_{year}{month}{day}.json"
    try:
        s3.head_object(Bucket=bucket, Key=key)
        return True
    except:
        return False


def process_date(date_str: str, index: int, total: int) -> dict:
    """Process a single date — fetch + upload both datasets."""
    try:
        weather_data = fetch_weather(date_str)
        upload_weather(weather_data, date_str)

        energy_data = fetch_energy(date_str)
        upload_energy(energy_data, date_str)

        print(f"[{index}/{total}] ✅ {date_str}")
        return {"date": date_str, "status": "success"}

    except Exception as e:
        print(f"[{index}/{total}] ❌ {date_str} — {e}")
        return {"date": date_str, "status": "failed", "error": str(e)}


def daterange(start_date: str, end_date: str):
    start   = datetime.strptime(start_date, "%Y-%m-%d")
    end     = datetime.strptime(end_date,   "%Y-%m-%d")
    current = start
    while current <= end:
        yield current.strftime("%Y-%m-%d")
        current += timedelta(days=1)


def backfill(start_date: str, end_date: str, max_workers: int = 8):
    all_dates = list(daterange(start_date, end_date))

    # Filter out dates already in S3
    print("🔍 Checking which dates already exist in S3...")
    pending = [d for d in all_dates if not date_exists_in_s3(d)]
    skipped = len(all_dates) - len(pending)

    print(f"📅 Total dates:   {len(all_dates)}")
    print(f"⏭️  Already done:  {skipped}")
    print(f"🔄 To process:    {len(pending)}")
    print(f"⚡ Workers:       {max_workers}\n")

    if not pending:
        print("✅ All dates already backfilled!")
        return

    success, failed = 0, 0
    start_time = time.time()

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {
            executor.submit(process_date, date_str, i+1, len(pending)): date_str
            for i, date_str in enumerate(pending)
        }

        for future in as_completed(futures):
            result = future.result()
            if result["status"] == "success":
                success += 1
            else:
                failed += 1

            # Progress update every 50 dates
            done = success + failed
            if done % 50 == 0:
                elapsed  = time.time() - start_time
                rate     = done / elapsed
                remaining = (len(pending) - done) / rate / 60
                print(f"\n📊 Progress: {done}/{len(pending)} | "
                      f"Rate: {rate:.1f} dates/sec | "
                      f"ETA: {remaining:.0f} min\n")

    elapsed = (time.time() - start_time) / 60
    print(f"\n{'='*50}")
    print(f"✅ Success:  {success}")
    print(f"❌ Failed:   {failed}")
    print(f"⏱️  Time:     {elapsed:.1f} minutes")


if __name__ == "__main__":
    START_DATE = "2023-01-01"
    END_DATE   = (datetime.utcnow() - timedelta(days=1)).strftime("%Y-%m-%d")

    backfill(START_DATE, END_DATE, max_workers=1)
