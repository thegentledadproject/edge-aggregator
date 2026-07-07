# polymarket_source.py
"""Live Polymarket CLOB data source: per-station market/token registry + REST client."""
import requests

CLOB_BASE_URL = "https://clob.polymarket.com"

# Maps each weather station to the Polymarket temperature-bucket markets that
# track it. Each entry's token_id is the CLOB token for that bucket's "Yes"
# outcome. Replace these placeholder token_ids with the live condition's real
# token IDs (from the Gamma API: https://gamma-api.polymarket.com/markets)
# before trading on this in production.
STATION_MARKETS = {
    "KORD": [
        {"bucket": "75-77°F", "token_id": "0x2a8e991cf3f1"},
        {"bucket": "78-80°F", "token_id": "0x3b9f002dg4h2"},
        {"bucket": "82-84°F", "token_id": "0x4f7d223ab8d1"},
        {"bucket": "85-87°F", "token_id": "0x7e2a4411bc89"},
    ],
    "KNYC": [
        {"bucket": "68-70°F", "token_id": "0x1c7a883ef2e0"},
        {"bucket": "71-73°F", "token_id": "0x2d8b994fa3f1"},
        {"bucket": "74-76°F", "token_id": "0x3e9ca05ab4a2"},
    ],
    "KAUS": [
        {"bucket": "95-97°F", "token_id": "0x5f1de17bc5b3"},
        {"bucket": "98-100°F", "token_id": "0x602ef28cd6c4"},
        {"bucket": "101-103°F", "token_id": "0x713f039de7d5"},
    ],
}


def fetch_market_prices(station_id: str) -> list:
    """Fetch live midpoint prices for every tracked bucket at a station."""
    if station_id not in STATION_MARKETS:
        raise ValueError(
            f"Unknown station_id {station_id!r}; add its markets to STATION_MARKETS."
        )

    contracts = []
    for contract in STATION_MARKETS[station_id]:
        response = requests.get(
            f"{CLOB_BASE_URL}/midpoint",
            params={"token_id": contract["token_id"]},
            timeout=10,
        )
        response.raise_for_status()
        price = float(response.json()["mid"])
        contracts.append({
            "bucket": contract["bucket"],
            "price": price,
            "token_id": contract["token_id"],
        })
    return contracts
