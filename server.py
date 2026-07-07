# server.py
import asyncio
from fastapi import FastAPI, Header, HTTPException
from contextlib import asynccontextmanager
from aggregator import IngestionEngine, CalibrationAndEdgeCore, FreemiumGateway

# Thread-safe in-memory global state array
GLOBAL_ALPHA_CACHE = {
    "data": [],
    "last_updated": None
}

# Production API Key Database Mock
VALID_PREMIUM_KEYS = {"sk_live_weather_edge_alpha_99", "sk_live_internal_puchong_node"}

@asynccontextmanager
async def app_lifespan(app: FastAPI):
    """Asynchronous continuous collection background loop"""
    ingest = IngestionEngine()
    core = CalibrationAndEdgeCore()
    
    async def statistical_calculation_worker():
        loop = asyncio.get_running_loop()
        while True:
            try:
                # Layer 1: Ingest (fetch_weather_matrix does blocking network I/O,
                # so it's offloaded to a worker thread to avoid stalling the event loop)
                weather_raw = await loop.run_in_executor(None, ingest.fetch_weather_matrix, "KORD")
                market_raw = ingest.fetch_polymarket_clob("m-kord-temp-2026")
                
                # Layer 2: Compute and record database tracking
                calculated_alpha = core.compute_gaussian_edges(weather_raw, market_raw)
                
                # Update local high-speed atomic memory cache
                GLOBAL_ALPHA_CACHE["data"] = calculated_alpha
                GLOBAL_ALPHA_CACHE["last_updated"] = weather_raw["timestamp_utc"]
                
            except Exception as e:
                print(f"[DAEMON WORKER ERROR EXECUTION EXCEPTION]: {str(e)}")
            
            # Non-blocking async sleep frequency throttling interval (10 seconds)
            await asyncio.sleep(10)

    # Fire the loop thread inside the active process async execution window
    worker_task = asyncio.create_task(statistical_calculation_worker())
    yield
    # Cleanup background processes safely on node shutdown signals
    worker_task.cancel()

app = FastAPI(title="Weather Edge Engine Node", lifespan=app_lifespan)


@app.get("/api/v1/weather/edges")
async def get_alpha_matrix(x_api_key: str = Header(default=None)):
    """FastAPI Routing Entry Endpoint"""
    if not GLOBAL_ALPHA_CACHE["data"]:
        raise HTTPException(status_code=503, detail="Cache warming up. Try again in 10s.")

    # Determine structural access rights via authorization header check
    is_premium = x_api_key in VALID_PREMIUM_KEYS
    
    # Layer 3 Dynamic Redaction Filtering Layer
    secured_payload = FreemiumGateway.apply_tier_mask(
        GLOBAL_ALPHA_CACHE["data"], 
        is_premium=is_premium
    )
    
    return {
        "status": "success",
        "tier_context": "premium" if is_premium else "free_unauthenticated",
        "telemetry": {
            "cache_timestamp_utc": GLOBAL_ALPHA_CACHE["last_updated"]
        },
        "results": secured_payload
    }