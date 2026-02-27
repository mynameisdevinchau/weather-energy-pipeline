import requests
import json
import os
from datetime import datetime, timedelta
from dotenv import load_dotenv

load_dotenv()

EIA_API_KEY = os.getenv("EIA_API_KEY")

# EIA region codes mapped to our cities
REGIONS = [
    {"city": "New York",    "region_id": "NYIS"},
    {"city": "Chicago",     "region_id": "MISO"},
    {"city": "Houston",     "region_id": "ERCO"},
    {"city": "Phoenix",     "region_id": "AZPS"},
    {"city": "Los Angeles", "region_id": "CISO"},
]

def fetch_energy(date_str: str = None) -> list[dict]:
    """
    Fetch daily average electricity demand (MWh) per region.
    date_str format: 'YYYY-MM-DD'. Defaults to yesterday.
    """
    if date_str is None:
        date_str = (datetime.utcnow() - timedelta(days=1)).strftime("%Y-%m-%d")

    if not EIA_API_KEY:
        raise ValueError("EIA_API_KEY not found. Check your .env file.")

    results = []

    for region in REGIONS:
        url = "https://api.eia.gov/v2/electricity/rto/region-data/data/"
        params = {
            "api_key":          EIA_API_KEY,
            "frequency":        "hourly",
            "data[0]":          "value",
            "facets[respondent][]": region["region_id"],
            "facets[type][]":   "D",   # D = Demand
            "start":            f"{date_str}T00",
            "end":              f"{date_str}T23",
            "sort[0][column]":  "period",
            "sort[0][direction]": "asc",
            "length":           24,
        }

        response = requests.get(url, params=params)
        response.raise_for_status()
        raw = response.json()

        records = raw.get("response", {}).get("data", [])

        if not records:
            print(f"No energy data for {region['city']} on {date_str}")
            continue

        values = [float(r["value"]) for r in records if r.get("value") is not None]
        avg_demand = round(sum(values) / len(values), 2) if values else None
        max_demand = max(values) if values else None
        min_demand = min(values) if values else None

        record = {
            "city":            region["city"],
            "region_id":       region["region_id"],
            "date":            date_str,
            "avg_demand_mwh":  avg_demand,
            "max_demand_mwh":  max_demand,
            "min_demand_mwh":  min_demand,
            "hours_collected": len(values),
            "ingested_at":     datetime.utcnow().isoformat(),
        }

        results.append(record)
        print(f"Energy fetched for {region['city']} ({region['region_id']}) on {date_str}")

    return results


if __name__ == "__main__":
    data = fetch_energy()
    print(json.dumps(data, indent=2))