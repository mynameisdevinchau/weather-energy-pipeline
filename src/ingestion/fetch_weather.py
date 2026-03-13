import requests
import json
import os
import time
from datetime import datetime, timedelta
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

CITIES = [
    {"name": "New York",        "lat": 40.7128,  "lon": -74.0060},
    {"name": "Los Angeles",     "lat": 34.0522,  "lon": -118.2437},
    {"name": "Chicago",         "lat": 41.8781,  "lon": -87.6298},
    {"name": "Houston",         "lat": 29.7604,  "lon": -95.3698},
    {"name": "Phoenix",         "lat": 33.4484,  "lon": -112.0740},
    {"name": "Philadelphia",    "lat": 39.9526,  "lon": -75.1652},
    {"name": "San Antonio",     "lat": 29.4241,  "lon": -98.4936},
    {"name": "San Diego",       "lat": 32.7157,  "lon": -117.1611},
    {"name": "Dallas",          "lat": 32.7767,  "lon": -96.7970},
    {"name": "San Jose",        "lat": 37.3382,  "lon": -121.8863},
    {"name": "Austin",          "lat": 30.2672,  "lon": -97.7431},
    {"name": "Jacksonville",    "lat": 30.3322,  "lon": -81.6557},
    {"name": "Fort Worth",      "lat": 32.7555,  "lon": -97.3308},
    {"name": "Columbus",        "lat": 39.9612,  "lon": -82.9988},
    {"name": "Charlotte",       "lat": 35.2271,  "lon": -80.8431},
    {"name": "Indianapolis",    "lat": 39.7684,  "lon": -86.1581},
    {"name": "San Francisco",   "lat": 37.7749,  "lon": -122.4194},
    {"name": "Seattle",         "lat": 47.6062,  "lon": -122.3321},
    {"name": "Denver",          "lat": 39.7392,  "lon": -104.9903},
    {"name": "Nashville",       "lat": 36.1627,  "lon": -86.7816},
    {"name": "Oklahoma City",   "lat": 35.4676,  "lon": -97.5164},
    {"name": "El Paso",         "lat": 31.7619,  "lon": -106.4850},
    {"name": "Washington DC",   "lat": 38.9072,  "lon": -77.0369},
    {"name": "Las Vegas",       "lat": 36.1699,  "lon": -115.1398},
    {"name": "Louisville",      "lat": 38.2527,  "lon": -85.7585},
    {"name": "Memphis",         "lat": 35.1495,  "lon": -90.0490},
    {"name": "Portland",        "lat": 45.5051,  "lon": -122.6750},
    {"name": "Baltimore",       "lat": 39.2904,  "lon": -76.6122},
    {"name": "Milwaukee",       "lat": 43.0389,  "lon": -87.9065},
    {"name": "Albuquerque",     "lat": 35.0844,  "lon": -106.6504},
    {"name": "Tucson",          "lat": 32.2226,  "lon": -110.9747},
    {"name": "Fresno",          "lat": 36.7378,  "lon": -119.7871},
    {"name": "Sacramento",      "lat": 38.5816,  "lon": -121.4944},
    {"name": "Kansas City",     "lat": 39.0997,  "lon": -94.5786},
    {"name": "Atlanta",         "lat": 33.7490,  "lon": -84.3880},
    {"name": "Miami",           "lat": 25.7617,  "lon": -80.1918},
    {"name": "Minneapolis",     "lat": 44.9778,  "lon": -93.2650},
    {"name": "Cleveland",       "lat": 41.4993,  "lon": -81.6944},
    {"name": "New Orleans",     "lat": 29.9511,  "lon": -90.0715},
    {"name": "Tampa",           "lat": 27.9506,  "lon": -82.4572},
    {"name": "Pittsburgh",      "lat": 40.4406,  "lon": -79.9959},
    {"name": "St. Louis",       "lat": 38.6270,  "lon": -90.1994},
    {"name": "Cincinnati",      "lat": 39.1031,  "lon": -84.5120},
    {"name": "Raleigh",         "lat": 35.7796,  "lon": -78.6382},
    {"name": "Virginia Beach",  "lat": 36.8529,  "lon": -75.9780},
    {"name": "Omaha",           "lat": 41.2565,  "lon": -95.9345},
    {"name": "Richmond",        "lat": 37.5407,  "lon": -77.4360},
    {"name": "Boise",           "lat": 43.6150,  "lon": -116.2023},
    {"name": "Salt Lake City",  "lat": 40.7608,  "lon": -111.8910},
    {"name": "Buffalo",         "lat": 42.8864,  "lon": -78.8784},
]


def fetch_weather_range(start_date: str, end_date: str) -> dict[str, list[dict]]:
    """
    Fetch weather for ALL cities for a full date range in one call per city.
    Returns a dict of {date_str: [records]} for easy upload.
    """
    session = requests.Session()
    adapter = HTTPAdapter(max_retries=Retry(total=5, backoff_factor=2))
    session.mount("https://", adapter)

    # Collect all data keyed by date
    results_by_date = {}

    for city in CITIES:
        url = "https://archive-api.open-meteo.com/v1/archive"
        params = {
            "latitude":         city["lat"],
            "longitude":        city["lon"],
            "start_date":       start_date,
            "end_date":         end_date,
            "daily":            "temperature_2m_max,temperature_2m_min,temperature_2m_mean,precipitation_sum,windspeed_10m_max",
            "temperature_unit": "fahrenheit",
            "timezone":         "America/New_York",
        }

        response  = None
        last_error = None
        for attempt in range(6):
            try:
                response = session.get(url, params=params, timeout=60)
                if response.status_code == 429:
                    wait = 2 ** attempt
                    print(f"⏳ Rate limited for {city['name']}, waiting {wait}s...")
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

        # Raise loudly if all retries failed
        if response is None:
            raise RuntimeError(f"All 6 attempts failed for {city['name']}: {last_error}")

        raw   = response.json()
        daily = raw.get("daily", {})
        dates = daily.get("time", [])

        for i, date_str in enumerate(dates):
            record = {
                "city":              city["name"],
                "date":              date_str,
                "temp_max_f":        daily.get("temperature_2m_max",  [None])[i],
                "temp_min_f":        daily.get("temperature_2m_min",  [None])[i],
                "temp_mean_f":       daily.get("temperature_2m_mean", [None])[i],
                "precipitation_mm":  daily.get("precipitation_sum",   [None])[i],
                "windspeed_max_kmh": daily.get("windspeed_10m_max",   [None])[i],
                "ingested_at":       datetime.utcnow().isoformat(),
            }
            if date_str not in results_by_date:
                results_by_date[date_str] = []
            results_by_date[date_str].append(record)

        print(f"✅ Weather fetched for {city['name']} ({len(dates)} days)")
        time.sleep(0.5)  # gentle delay between cities

    return results_by_date


def fetch_weather(date_str: str = None) -> list[dict]:
    """Single day fetch for Lambda compatibility."""
    if date_str is None:
        date_str = (datetime.utcnow() - timedelta(days=1)).strftime("%Y-%m-%d")
    result = fetch_weather_range(date_str, date_str)
    return result.get(date_str, [])


if __name__ == "__main__":
    data = fetch_weather()
    print(json.dumps(data, indent=2))