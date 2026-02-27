import json
import os
import sys

# Make sure local modules are importable inside Lambda
sys.path.insert(0, os.path.dirname(__file__))

from datetime import datetime, timedelta
from fetch_weather import fetch_weather
from fetch_energy  import fetch_energy
from upload_to_s3  import upload_weather, upload_energy


def handler(event, context):
    """
    AWS Lambda entry point.
    Fetches yesterday's weather + energy data and uploads to S3.
    """
    date_str = (datetime.utcnow() - timedelta(days=1)).strftime("%Y-%m-%d")

    print(f"🚀 Lambda triggered. Fetching data for {date_str}")

    results = {}

    try:
        weather_data = fetch_weather(date_str)
        uri = upload_weather(weather_data, date_str)
        results["weather"] = {"status": "success", "records": len(weather_data), "s3_uri": uri}
    except Exception as e:
        results["weather"] = {"status": "failed", "error": str(e)}
        print(f"❌ Weather pipeline failed: {e}")

    try:
        energy_data = fetch_energy(date_str)
        uri = upload_energy(energy_data, date_str)
        results["energy"] = {"status": "success", "records": len(energy_data), "s3_uri": uri}
    except Exception as e:
        results["energy"] = {"status": "failed", "error": str(e)}
        print(f"❌ Energy pipeline failed: {e}")

    print(json.dumps(results, indent=2))

    return {
        "statusCode": 200,
        "body": json.dumps(results)
    }