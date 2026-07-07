# dashboard.py
"""Standalone diagnostic dashboard: runs one pipeline cycle and prints the tier views."""
from aggregator import IngestionEngine, CalibrationAndEdgeCore, FreemiumGateway


def run_diagnostic():
    print("Executing internal diagnostics test...")
    ingest = IngestionEngine()
    core = CalibrationAndEdgeCore()

    w_raw = ingest.fetch_weather_matrix("KORD")
    m_raw = ingest.fetch_polymarket_clob("KORD")
    calculated = core.compute_gaussian_edges(w_raw, m_raw)

    print("\n--- Diagnostic Mask View: Free Tier User ---")
    print(FreemiumGateway.apply_tier_mask(calculated, is_premium=False))

    print("\n--- Diagnostic Mask View: Premium Tier User ---")
    print(FreemiumGateway.apply_tier_mask(calculated, is_premium=True))

    print("\n[SUCCESS] Local pipeline diagnostic test complete. Database initialized.")


if __name__ == "__main__":
    run_diagnostic()
