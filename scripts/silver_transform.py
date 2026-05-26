"""
Silver layer — cleaning and normalisation.

Reads the bronze JSON, maps columns by name (not position) to guard against
API schema drift, drops rows with null velocity or country, casts types, and
writes a timestamped CSV so every 30-min window is preserved independently.
"""

import json
import logging
from pathlib import Path

import pandas as pd

log = logging.getLogger(__name__)

SILVER_DIR = Path("/opt/airflow/data/silver")

# OpenSky API field definitions (index → name).
# Using a dict allows safe mapping even if the API adds new trailing fields.
OPENSKY_COLUMNS = {
    0: "icao24",
    1: "callsign",
    2: "origin_country",
    3: "time_position",
    4: "last_contact",
    5: "longitude",
    6: "latitude",
    7: "baro_altitude",
    8: "on_ground",
    9: "velocity",
    10: "true_track",
    11: "vertical_rate",
    12: "sensors",
    13: "geo_altitude",
    14: "squawk",
    15: "spi",
    16: "position_source",
}

SILVER_COLUMNS = ["icao24", "origin_country", "velocity", "baro_altitude", "latitude", "longitude", "on_ground"]


def run_silver_transform(**context):
    SILVER_DIR.mkdir(parents=True, exist_ok=True)

    bronze_file = context["ti"].xcom_pull(key="bronze_file", task_ids="bronze_ingest")
    if not bronze_file:
        raise ValueError("Bronze file path not found in XCom.")

    with open(bronze_file) as f:
        raw = json.load(f)

    states = raw.get("states")
    if not states:
        raise ValueError(f"No states found in bronze file: {bronze_file}")

    # Map columns by index → name so extra trailing API fields don't shift everything
    df_raw = pd.DataFrame(states)
    rename_map = {i: name for i, name in OPENSKY_COLUMNS.items() if i < len(df_raw.columns)}
    df_raw = df_raw.rename(columns=rename_map)

    # Keep only the columns we need (subset that definitely exist)
    available = [c for c in SILVER_COLUMNS if c in df_raw.columns]
    df = df_raw[available].copy()

    # Type casting
    df["velocity"] = pd.to_numeric(df["velocity"], errors="coerce")
    df["baro_altitude"] = pd.to_numeric(df["baro_altitude"], errors="coerce")
    df["latitude"] = pd.to_numeric(df["latitude"], errors="coerce")
    df["longitude"] = pd.to_numeric(df["longitude"], errors="coerce")
    # on_ground is bool in the API — normalise to int (1/0) for downstream agg
    df["on_ground"] = df["on_ground"].astype(bool).astype(int)

    # Drop rows missing the fields critical for aggregation
    before = len(df)
    df = df.dropna(subset=["origin_country", "velocity"])
    dropped = before - len(df)
    if dropped:
        log.warning("Dropped %d rows with null origin_country or velocity.", dropped)

    if df.empty:
        raise ValueError("Silver DataFrame is empty after cleaning — nothing to write.")

    log.info("Silver: %d rows retained from %d raw states.", len(df), before)

    # Use execution timestamp so each 30-min window has its own file (not overwritten)
    ts = context["data_interval_start"].strftime("%Y%m%d%H%M%S")
    output_file = SILVER_DIR / f"flights_silver_{ts}.csv"
    df.to_csv(output_file, index=False)

    log.info("Silver file written: %s", output_file)
    context["ti"].xcom_push(key="silver_file", value=str(output_file))
