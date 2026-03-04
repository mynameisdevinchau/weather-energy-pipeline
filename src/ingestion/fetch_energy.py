import requests
import json
import os
from datetime import datetime, timedelta
from dotenv import load_dotenv
import time

load_dotenv()

EIA_API_KEY = os.getenv("EIA_API_KEY")

# EIA region codes mapped to our cities
REGIONS = [
    # NYIS — New York
    {"city": "New York",        "region_id": "NYIS"},
    {"city": "Buffalo",         "region_id": "NYIS"},

    # ISNE — New England
    {"city": "Philadelphia",    "region_id": "ISNE"},
    {"city": "Baltimore",       "region_id": "ISNE"},
    {"city": "Washington DC",   "region_id": "ISNE"},
    {"city": "Richmond",        "region_id": "ISNE"},
    {"city": "Virginia Beach",  "region_id": "ISNE"},
    {"city": "Raleigh",         "region_id": "ISNE"},

    # MISO — Midwest
    {"city": "Chicago",         "region_id": "MISO"},
    {"city": "Indianapolis",    "region_id": "MISO"},
    {"city": "Columbus",        "region_id": "MISO"},
    {"city": "Cleveland",       "region_id": "MISO"},
    {"city": "Milwaukee",       "region_id": "MISO"},
    {"city": "Minneapolis",     "region_id": "MISO"},
    {"city": "Kansas City",     "region_id": "MISO"},
    {"city": "St. Louis",       "region_id": "MISO"},
    {"city": "Memphis",         "region_id": "MISO"},
    {"city": "New Orleans",     "region_id": "MISO"},
    {"city": "Omaha",           "region_id": "MISO"},

    # PJM — Mid-Atlantic/Midwest
    {"city": "Pittsburgh",      "region_id": "PJM"},
    {"city": "Cincinnati",      "region_id": "PJM"},
    {"city": "Louisville",      "region_id": "PJM"},
    {"city": "Charlotte",       "region_id": "PJM"},

    # ERCO — Texas
    {"city": "Houston",         "region_id": "ERCO"},
    {"city": "Dallas",          "region_id": "ERCO"},
    {"city": "San Antonio",     "region_id": "ERCO"},
    {"city": "Austin",          "region_id": "ERCO"},
    {"city": "Fort Worth",      "region_id": "ERCO"},
    {"city": "El Paso",         "region_id": "ERCO"},

    # AZPS — Southwest
    {"city": "Phoenix",         "region_id": "AZPS"},
    {"city": "Tucson",          "region_id": "AZPS"},
    {"city": "Albuquerque",     "region_id": "AZPS"},
    {"city": "Las Vegas",       "region_id": "AZPS"},
    {"city": "Salt Lake City",  "region_id": "AZPS"},

    # CISO — California
    {"city": "Los Angeles",     "region_id": "CISO"},
    {"city": "San Diego",       "region_id": "CISO"},
    {"city": "San Jose",        "region_id": "CISO"},
    {"city": "San Francisco",   "region_id": "CISO"},
    {"city": "Fresno",          "region_id": "CISO"},
    {"city": "Sacramento",      "region_id": "CISO"},

    # PACW — Pacific Northwest
    {"city": "Seattle",         "region_id": "PACW"},
    {"city": "Portland",        "region_id": "PACW"},
    {"city": "Boise",           "region_id": "PACW"},

    # SWPP — Central
    {"city": "Oklahoma City",   "region_id": "SWPP"},
    {"city": "Denver",          "region_id": "SWPP"},

    # Southeast
    {"city": "Atlanta",         "region_id": "SOCO"},
    {"city": "Miami",           "region_id": "FPL"},
    {"city": "Tampa",           "region_id": "TEC"},
    {"city": "Jacksonville",    "region_id": "FPL"},
    {"city": "Nashville",       "region_id": "TVA"},
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

        for attempt in range(5):
            try:
                response = requests.get(url, params=params, timeout=30)
                if response.status_code in (429, 504):
                    wait = 2 ** attempt
                    print(f"⏳ Rate limited/timeout for {region['city']}, waiting {wait}s...")
                    time.sleep(wait)
                    continue
                response.raise_for_status()
                break
            except requests.exceptions.ConnectionError:
                time.sleep(2 ** attempt)
                continue
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