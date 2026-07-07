# weather_source.py
"""Live weather data source: station registry + Open-Meteo API client."""
import requests

# Ground-station coordinates for the weather stations tracked by the
# Polymarket temperature markets. Add an entry here before fetching
# a new station_id.
STATION_COORDINATES = {
    "KORD": (41.9786, -87.9048),   # Chicago O'Hare Intl
    "KNYC": (40.7794, -73.9692),   # Central Park, NYC
    "KAUS": (30.1975, -97.6664),   # Austin-Bergstrom Intl
    "KMIA": (25.7959, -80.2870),   # Miami Intl
    "KLAX": (33.9382, -118.3866),  # LAX
    "KDEN": (39.8461, -104.6737),  # Denver Intl
}

OPEN_METEO_URL = "https://api.open-meteo.com/v1/forecast"


def fetch_station_temperatures(station_id: str) -> list:
    """Fetch today's hourly Fahrenheit temperature samples for a station_id."""
    if station_id not in STATION_COORDINATES:
        raise ValueError(
            f"Unknown station_id {station_id!r}; add its coordinates to STATION_COORDINATES."
        )
    latitude, longitude = STATION_COORDINATES[station_id]

    response = requests.get(
        OPEN_METEO_URL,
        params={
            "latitude": latitude,
            "longitude": longitude,
            "hourly": "temperature_2m",
            "temperature_unit": "fahrenheit",
            "forecast_days": 1,
            "timezone": "UTC",
        },
        timeout=10,
    )
    response.raise_for_status()
    payload = response.json()
    return payload["hourly"]["temperature_2m"]
