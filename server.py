# server.py
import asyncio
import os
from fastapi import FastAPI, Header, HTTPException, Request
from contextlib import asynccontextmanager
from aggregator import IngestionEngine, CalibrationAndEdgeCore, FreemiumGateway
from weather_source import STATION_COORDINATES
from polymarket_source import STATION_MARKETS

# Stations polled every cycle: need both ground-station coordinates (weather_source)
# and a configured CLOB market registry (polymarket_source) to be pollable.
POLLED_STATIONS = sorted(set(STATION_COORDINATES) & set(STATION_MARKETS))

# Thread-safe in-memory global state, keyed by station_id
GLOBAL_ALPHA_CACHE = {
    "stations": {station_id: {"data": [], "last_updated": None} for station_id in POLLED_STATIONS}
}

# Premium API keys, comma-separated in the VALID_PREMIUM_KEYS env var
# (e.g. "sk_live_abc,sk_live_def"). No keys are granted premium access if unset.
VALID_PREMIUM_KEYS = {
    key.strip() for key in os.getenv("VALID_PREMIUM_KEYS", "").split(",") if key.strip()
}

@asynccontextmanager
async def app_lifespan(app: FastAPI):
    """Asynchronous continuous collection background loop"""
    ingest = IngestionEngine()
    core = CalibrationAndEdgeCore()
    # Single source of truth for the calibration core; route handlers read it
    # off app.state rather than a separate module-level global.
    app.state.core = core

    async def poll_station(loop, station_id):
        # Layer 1: Ingest (both calls do blocking network I/O, so they're
        # offloaded to worker threads to avoid stalling the event loop)
        weather_raw = await loop.run_in_executor(None, ingest.fetch_weather_matrix, station_id)
        market_raw = await loop.run_in_executor(None, ingest.fetch_polymarket_clob, station_id)

        # Layer 2: Compute and record database tracking
        calculated_alpha = core.compute_gaussian_edges(weather_raw, market_raw)

        # Update local high-speed atomic memory cache
        GLOBAL_ALPHA_CACHE["stations"][station_id] = {
            "data": calculated_alpha,
            "last_updated": weather_raw["timestamp_utc"],
        }

    async def statistical_calculation_worker():
        loop = asyncio.get_running_loop()
        while True:
            # Poll every station concurrently; one station's failure shouldn't
            # block the others from refreshing this cycle.
            results = await asyncio.gather(
                *(poll_station(loop, station_id) for station_id in POLLED_STATIONS),
                return_exceptions=True,
            )
            for station_id, result in zip(POLLED_STATIONS, results):
                if isinstance(result, Exception):
                    print(f"[DAEMON WORKER ERROR EXECUTION EXCEPTION] {station_id}: {result}")

            # Non-blocking async sleep frequency throttling interval (10 seconds)
            await asyncio.sleep(10)

    # Fire the loop thread inside the active process async execution window
    worker_task = asyncio.create_task(statistical_calculation_worker())
    yield
    # Cleanup background processes safely on node shutdown signals
    worker_task.cancel()

app = FastAPI(title="Weather Edge Engine Node", lifespan=app_lifespan)


@app.get("/api/v1/weather/edges")
async def get_all_alpha_matrices(x_api_key: str = Header(default=None)):
    """Aggregate snapshot across every polled station."""
    is_premium = x_api_key in VALID_PREMIUM_KEYS

    stations_payload = {}
    for station_id, station_cache in GLOBAL_ALPHA_CACHE["stations"].items():
        if not station_cache["data"]:
            continue
        stations_payload[station_id] = {
            "telemetry": {"cache_timestamp_utc": station_cache["last_updated"]},
            "results": FreemiumGateway.apply_tier_mask(station_cache["data"], is_premium=is_premium),
        }

    if not stations_payload:
        raise HTTPException(status_code=503, detail="Cache warming up. Try again in 10s.")

    return {
        "status": "success",
        "tier_context": "premium" if is_premium else "free_unauthenticated",
        "stations": stations_payload,
    }


@app.get("/api/v1/weather/edges/{station_id}")
async def get_alpha_matrix(station_id: str, x_api_key: str = Header(default=None)):
    """FastAPI Routing Entry Endpoint for a single station."""
    station_cache = GLOBAL_ALPHA_CACHE["stations"].get(station_id)
    if station_cache is None:
        raise HTTPException(status_code=404, detail=f"Unknown or unpolled station_id {station_id!r}.")
    if not station_cache["data"]:
        raise HTTPException(status_code=503, detail="Cache warming up. Try again in 10s.")

    # Determine structural access rights via authorization header check
    is_premium = x_api_key in VALID_PREMIUM_KEYS

    # Layer 3 Dynamic Redaction Filtering Layer
    secured_payload = FreemiumGateway.apply_tier_mask(
        station_cache["data"],
        is_premium=is_premium
    )

    return {
        "status": "success",
        "tier_context": "premium" if is_premium else "free_unauthenticated",
        "telemetry": {
            "cache_timestamp_utc": station_cache["last_updated"]
        },
        "results": secured_payload
    }


@app.get("/api/v1/weather/edges/history/{token_id}")
async def get_edge_history(request: Request, token_id: str, x_api_key: str = Header(default=None), limit: int = 100):
    """Per-contract edge history: how model_prob/market_price/expected_value moved over time."""
    is_premium = x_api_key in VALID_PREMIUM_KEYS

    rows = request.app.state.core.get_edge_history(token_id, limit=limit)
    # Reshape rows to match the FreemiumGateway's expected node structure, then
    # apply the same free/premium redaction rule used on the live matrix.
    normalized = [
        {
            "bucket": row["bucket"],
            "market_price": row["market_price"],
            "model_prob": row["model_prob"],
            "expected_value": row["expected_value"],
            "token_id": row["token_id"],
            "generated_at_utc": row["timestamp"],
        }
        for row in rows
    ]
    secured_history = FreemiumGateway.apply_tier_mask(normalized, is_premium=is_premium)

    return {
        "status": "success",
        "tier_context": "premium" if is_premium else "free_unauthenticated",
        "token_id": token_id,
        "count": len(secured_history),
        "history": secured_history
    }