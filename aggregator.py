# aggregator.py
import sqlite3
import re
import time
import math
import statistics
import requests
from datetime import datetime

# Ground-station coordinates for the weather stations tracked by the
# Polymarket temperature markets. Add an entry here before calling
# fetch_weather_matrix() with a new station_id.
STATION_COORDINATES = {
    "KORD": (41.9786, -87.9048),   # Chicago O'Hare Intl
    "KNYC": (40.7794, -73.9692),   # Central Park, NYC
    "KAUS": (30.1975, -97.6664),   # Austin-Bergstrom Intl
    "KMIA": (25.7959, -80.2870),   # Miami Intl
    "KLAX": (33.9382, -118.3866),  # LAX
    "KDEN": (39.8461, -104.6737),  # Denver Intl
}

OPEN_METEO_URL = "https://api.open-meteo.com/v1/forecast"


class IngestionEngine:
    """Layer 1: Raw Data Transport Map Engine"""
    def __init__(self):
        pass

    def fetch_weather_matrix(self, station_id: str) -> dict:
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
        samples = payload["hourly"]["temperature_2m"]

        return {
            "station_id": station_id,
            "raw_temp_samples": samples,
            "timestamp_utc": int(time.time())
        }

    def fetch_polymarket_clob(self, market_id: str) -> list:
        # Simulating external raw Central Limit Order Book (CLOB) payload arrays
        return [
            {"bucket": "75-77°F", "price": 0.35, "token_id": "0x2a8e991cf3f1"},
            {"bucket": "78-80°F", "price": 0.45, "token_id": "0x3b9f002dg4h2"},
            {"bucket": "82-84°F", "price": 0.12, "token_id": "0x4f7d223ab8d1"},
            {"bucket": "85-87°F", "price": 0.60, "token_id": "0x7e2a4411bc89"}
        ]


class CalibrationAndEdgeCore:
    """Layer 2: Statistical Calculation Core & SQLite Tracker"""

    _BUCKET_RANGE_RE = re.compile(r"(-?\d+(?:\.\d+)?)\s*-\s*(-?\d+(?:\.\d+)?)")

    def __init__(self, db_path: str = "dashboard_alpha.db"):
        self.db_path = db_path
        self._init_db()

    def _init_db(self):
        with sqlite3.connect(self.db_path) as conn:
            conn.execute("""
                CREATE TABLE IF NOT EXISTS statistical_logs (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    timestamp TEXT,
                    station_id TEXT,
                    rmse REAL,
                    sigma REAL
                )
            """)
            conn.commit()

    @staticmethod
    def _parse_bucket_range(bucket: str) -> tuple:
        """Extract the (low, high) degF bounds from a label like '78-80°F'."""
        match = CalibrationAndEdgeCore._BUCKET_RANGE_RE.search(bucket)
        if not match:
            raise ValueError(f"Unable to parse temperature bucket: {bucket!r}")
        low, high = float(match.group(1)), float(match.group(2))
        return (low, high) if low <= high else (high, low)

    @staticmethod
    def _normal_cdf(x: float, mean: float, sigma: float) -> float:
        """Standard normal CDF via the error function (no scipy dependency)."""
        if sigma <= 0:
            return 1.0 if x >= mean else 0.0
        return 0.5 * (1.0 + math.erf((x - mean) / (sigma * math.sqrt(2))))

    def compute_gaussian_edges(self, weather_data: dict, market_data: list) -> list:
        samples = weather_data["raw_temp_samples"]
        mean_temp = statistics.fmean(samples)

        # Sample standard deviation of the raw readings, and the standard
        # error of that mean (sigma / sqrt(n)) as our RMSE proxy for the
        # station's forecast uncertainty.
        sigma = statistics.stdev(samples) if len(samples) > 1 else 0.0
        rmse = sigma / math.sqrt(len(samples)) if samples else 0.0

        # Write validation metrics to database (thread-safe inside lifecycle loop)
        with sqlite3.connect(self.db_path) as conn:
            conn.execute(
                "INSERT INTO statistical_logs (timestamp, station_id, rmse, sigma) VALUES (?, ?, ?, ?)",
                (datetime.utcnow().isoformat(), weather_data["station_id"], rmse, sigma)
            )
            conn.commit()

        # Compute edge arrays: Expected Value (EV) = Model Probability - Market Price
        processed_matrix = []
        for contract in market_data:
            low, high = self._parse_bucket_range(contract["bucket"])
            # P(low <= temp <= high) under N(mean_temp, sigma) via the CDF.
            model_prob = self._normal_cdf(high, mean_temp, sigma) - self._normal_cdf(low, mean_temp, sigma)
            model_prob = max(0.0, min(1.0, model_prob))
            expected_value = model_prob - contract["price"]

            processed_matrix.append({
                "bucket": contract["bucket"],
                "market_price": contract["price"],
                "model_prob": round(model_prob, 2),
                "expected_value": round(expected_value, 2),
                "token_id": contract["token_id"],
                "generated_at_utc": weather_data["timestamp_utc"]
            })
        return processed_matrix


class FreemiumGateway:
    """Layer 3: Monetization and Privacy Redaction Mask"""
    @staticmethod
    def apply_tier_mask(data_array: list, is_premium: bool) -> list:
        if is_premium:
            return data_array

        masked_output = []
        # Free Tier Degradation Rule: blur downstream results, redact identifiers
        for index, node in enumerate(data_array):
            if index == 0:
                # Give a single sample node away, but drop raw contract address strings
                clean_node = node.copy()
                clean_node["token_id"] = "REDACTED_AUTHENTICATE_REQUIRED"
                masked_output.append(clean_node)
            else:
                # Obfuscate all subsequent rows completely
                masked_output.append({
                    "bucket": node["bucket"],
                    "market_price": node["market_price"],
                    "model_prob": "LOCKED",
                    "expected_value": "LOCKED",
                    "token_id": "LOCKED",
                    "generated_at_utc": "15_MINS_DELAYED"
                })
        return masked_output


if __name__ == "__main__":
    print("Executing internal diagnostics test...")
    ingest = IngestionEngine()
    core = CalibrationAndEdgeCore()
    
    w_raw = ingest.fetch_weather_matrix("KORD")
    m_raw = ingest.fetch_polymarket_clob("m-1234")
    calculated = core.compute_gaussian_edges(w_raw, m_raw)
    
    print("\n--- Diagnostic Mask View: Free Tier User ---")
    print(FreemiumGateway.apply_tier_mask(calculated, is_premium=False))
    print("\n[SUCCESS] Local pipeline diagnostic test complete. Database initialized.")