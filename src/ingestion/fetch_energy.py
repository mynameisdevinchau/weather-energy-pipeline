import requests
import json
import os
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed
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

# ── Key optimization: deduplicate regions ─────────────────────────────────────
# Multiple cities share the same region_id (e.g. 11 cities all map to MISO).
# We only need ONE API call per unique region, then fan the result out to all
# cities in that region. Reduces 50 API calls → 13 unique region calls.
UNIQUE_REGIONS = list({r["region_id"]: r for r in REGIONS}.keys())  # 13 unique
CITIES_BY_REGION = {}
for r in REGIONS:
    CITIES_BY_REGION.setdefault(r["region_id"], []).append(r["city"])


def _fetch_region(region_id: str, date_str: str) -> dict:
    """
    Fetch one region's hourly demand and return parsed stats.
    Called concurrently via ThreadPoolExecutor.
    Raises RuntimeError if all retries fail — caller handles it.
    """
    url = "https://api.eia.gov/v2/electricity/rto/region-data/data/"
    params = {
        "api_key":              EIA_API_KEY,
        "frequency":            "hourly",
        "data[0]":              "value",
        "facets[respondent][]": region_id,
        "facets[type][]":       "D",
        "start":                f"{date_str}T00",
        "end":                  f"{date_str}T23",
        "sort[0][column]":      "period",
        "sort[0][direction]":   "asc",
        "length":               24,
    }

    response   = None
    last_error = None

    for attempt in range(5):
        try:
            response = requests.get(url, params=params, timeout=30)
            if response.status_code in (429, 504):
                wait = 2 ** attempt
                print(f"⏳ Rate limited ({region_id}), waiting {wait}s...")
                time.sleep(wait)
                response = None
                continue
            response.raise_for_status()
            break
        except (requests.exceptions.ConnectionError,
                requests.exceptions.HTTPError) as e:
            last_error = e
            time.sleep(2 ** attempt)
            response = None
            continue

    if response is None:
        raise RuntimeError(f"All retries failed for region {region_id}: {last_error}")

    records = response.json().get("response", {}).get("data", [])
    values  = [float(r["value"]) for r in records if r.get("value") is not None]

    return {
        "region_id":       region_id,
        "avg_demand_mwh":  round(sum(values) / len(values), 2) if values else None,
        "max_demand_mwh":  max(values) if values else None,
        "min_demand_mwh":  min(values) if values else None,
        "hours_collected": len(values),
    }


def fetch_energy(date_str: str = None) -> list[dict]:
    """
    Fetch daily electricity demand for all 50 cities.

    Strategy: make 13 unique region API calls (not 50) using ThreadPoolExecutor,
    then fan each region's result out to all cities mapped to that region.
    Reduces API calls by ~74% and fits comfortably within Lambda's 300s timeout.
    """
    if date_str is None:
        date_str = (datetime.utcnow() - timedelta(days=1)).strftime("%Y-%m-%d")

    if not EIA_API_KEY:
        raise ValueError("EIA_API_KEY not found. Check your .env file.")

    # ── Fetch all 13 regions in parallel (4 workers — respectful of EIA rate limits)
    region_results = {}
    region_errors  = {}

    print(f"⚡ Fetching {len(UNIQUE_REGIONS)} unique regions in parallel for {date_str}...")

    with ThreadPoolExecutor(max_workers=4) as executor:
        future_to_region = {
            executor.submit(_fetch_region, region_id, date_str): region_id
            for region_id in UNIQUE_REGIONS
        }
        for future in as_completed(future_to_region):
            region_id = future_to_region[future]
            try:
                region_results[region_id] = future.result()
                print(f"  ✅ {region_id}")
            except Exception as e:
                region_errors[region_id] = str(e)
                print(f"  ❌ {region_id}: {e}")

    if region_errors:
        print(f"⚠️  {len(region_errors)} region(s) failed: {list(region_errors.keys())}")

    # ── Fan out region results to all cities in each region ───────────────────
    results = []
    ingested_at = datetime.utcnow().isoformat()

    for region_id, cities in CITIES_BY_REGION.items():
        if region_id not in region_results:
            # Region fetch failed — skip all cities in this region
            for city in cities:
                print(f"  ⏭️  Skipping {city} ({region_id}) — region fetch failed")
            continue

        stats = region_results[region_id]

        for city in cities:
            results.append({
                "city":            city,
                "region_id":       region_id,
                "date":            date_str,
                "avg_demand_mwh":  stats["avg_demand_mwh"],
                "max_demand_mwh":  stats["max_demand_mwh"],
                "min_demand_mwh":  stats["min_demand_mwh"],
                "hours_collected": stats["hours_collected"],
                "ingested_at":     ingested_at,
            })
            print(f"Energy fetched for {city} ({region_id}) on {date_str}")

    print(f"✅ Energy complete: {len(results)}/50 city records for {date_str}")
    return results


if __name__ == "__main__":
    data = fetch_energy()
    print(json.dumps(data, indent=2))