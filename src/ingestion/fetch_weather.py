import requests
import json
import os
from datetime import datetime, timedelta

# Cities we're tracking: name, lat, lon
CITIES = [
    {"name": "New York",    "lat": 40.7128, "lon": -74.0060},
    {"name": "Chicago",     "lat": 41.8781, "lon": -87.6298},
    {"name": "Houston",     "lat": 29.7604, "lon": -95.3698},
    {"name": "Phoenix",     "lat": 33.4484, "lon": -112.0740},
    {"name": "Los Angeles", "lat": 34.0522, "lon": -118.2437},
]

def fetch_weather(date_str: str = None) -> list[dict]:
    """
    Fetch daily weather data for all cities on a given date.
    date_str format: 'YYYY-MM-DD'. Defaults to yesterday.
    """
    if date_str is None:
        date_str = (datetime.utcnow() - timedelta(days=1)).strftime("%Y-%m-%d")

    results = []

    for city in CITIES:
        url = "https://archive-api.open-meteo.com/v1/archive"
        params = {
            "latitude":        city["lat"],
            "longitude":       city["lon"],
            "start_date":      date_str,
            "end_date":        date_str,
            "daily":           "temperature_2m_max,temperature_2m_min,temperature_2m_mean,precipitation_sum,windspeed_10m_max",
            "temperature_unit": "fahrenheit",
            "timezone":        "America/New_York",
        }

        response = requests.get(url, params=params)
        response.raise_for_status()
        raw = response.json()

        daily = raw.get("daily", {})

        record = {
            "city":            city["name"],
            "date":            date_str,
            "temp_max_f":      daily.get("temperature_2m_max", [None])[0],
            "temp_min_f":      daily.get("temperature_2m_min", [None])[0],
            "temp_mean_f":     daily.get("temperature_2m_mean", [None])[0],
            "precipitation_mm": daily.get("precipitation_sum", [None])[0],
            "windspeed_max_kmh": daily.get("windspeed_10m_max", [None])[0],
            "ingested_at":     datetime.utcnow().isoformat(),
        }

        results.append(record)
        print(f"✅ Weather fetched for {city['name']} on {date_str}")

    return results


if __name__ == "__main__":
    data = fetch_weather()
    print(json.dumps(data, indent=2))