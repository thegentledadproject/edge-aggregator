# aggregator.py
import sqlite3
import re
import time
import math
import statistics
from datetime import datetime

from weather_source import fetch_station_temperatures


class IngestionEngine:
    """Layer 1: Raw Data Transport Map Engine"""
    def __init__(self):
        pass

    def fetch_weather_matrix(self, station_id: str) -> dict:
        samples = fetch_station_temperatures(station_id)
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
            conn.execute("""
                CREATE TABLE IF NOT EXISTS edge_history (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    timestamp TEXT,
                    station_id TEXT,
                    token_id TEXT,
                    bucket TEXT,
                    market_price REAL,
                    model_prob REAL,
                    expected_value REAL
                )
            """)
            conn.execute("""
                CREATE INDEX IF NOT EXISTS idx_edge_history_token
                ON edge_history (token_id, timestamp)
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
        history_rows = []
        generated_at = datetime.utcnow().isoformat()
        for contract in market_data:
            low, high = self._parse_bucket_range(contract["bucket"])
            # P(low <= temp <= high) under N(mean_temp, sigma) via the CDF.
            model_prob = self._normal_cdf(high, mean_temp, sigma) - self._normal_cdf(low, mean_temp, sigma)
            model_prob = max(0.0, min(1.0, model_prob))
            expected_value = model_prob - contract["price"]
            model_prob, expected_value = round(model_prob, 2), round(expected_value, 2)

            processed_matrix.append({
                "bucket": contract["bucket"],
                "market_price": contract["price"],
                "model_prob": model_prob,
                "expected_value": expected_value,
                "token_id": contract["token_id"],
                "generated_at_utc": weather_data["timestamp_utc"]
            })
            history_rows.append((
                generated_at, weather_data["station_id"], contract["token_id"],
                contract["bucket"], contract["price"], model_prob, expected_value
            ))

        # Persist every bucket's edge for this cycle so per-token history can be queried later.
        with sqlite3.connect(self.db_path) as conn:
            conn.executemany(
                """INSERT INTO edge_history
                   (timestamp, station_id, token_id, bucket, market_price, model_prob, expected_value)
                   VALUES (?, ?, ?, ?, ?, ?, ?)""",
                history_rows
            )
            conn.commit()

        return processed_matrix

    def get_edge_history(self, token_id: str, limit: int = 100) -> list:
        """Return the most recent recorded edge snapshots for a single token, newest first."""
        with sqlite3.connect(self.db_path) as conn:
            conn.row_factory = sqlite3.Row
            rows = conn.execute(
                """SELECT timestamp, station_id, token_id, bucket, market_price, model_prob, expected_value
                   FROM edge_history
                   WHERE token_id = ?
                   ORDER BY id DESC
                   LIMIT ?""",
                (token_id, limit)
            ).fetchall()
        return [dict(row) for row in rows]


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
