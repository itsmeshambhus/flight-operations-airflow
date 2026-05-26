"""
Gold layer — aggregated KPIs per country per 30-minute window.

Reads the silver CSV for this window and produces one row per country with:
  - total_flights:      total aircraft observed
  - avg_velocity:       mean velocity (m/s) excluding nulls
  - on_ground_count:    number of aircraft on the ground
  - on_ground_pct:      percentage of aircraft on the ground (0–100)
"""

import logging
from pathlib import Path

import numpy as np
import pandas as pd

log = logging.getLogger(__name__)

GOLD_DIR = Path("/opt/airflow/data/gold")


def run_gold_aggregate(**context):
    GOLD_DIR.mkdir(parents=True, exist_ok=True)

    silver_file = context["ti"].xcom_pull(key="silver_file", task_ids="silver_transform")
    if not silver_file:
        raise ValueError("Silver file path not found in XCom.")

    df = pd.read_csv(silver_file)

    if df.empty:
        raise ValueError(f"Silver file is empty: {silver_file}")

    # on_ground was written as int (0/1) — ensure numeric for aggregation
    df["on_ground"] = pd.to_numeric(df["on_ground"], errors="coerce").fillna(0).astype(int)
    df["velocity"] = pd.to_numeric(df["velocity"], errors="coerce")

    agg = (
        df.groupby("origin_country")
        .agg(
            total_flights=("icao24", "count"),
            avg_velocity=("velocity", "mean"),        # nanmean by default in pandas
            on_ground_count=("on_ground", "sum"),
        )
        .reset_index()
    )

    # Derived metric — useful for BI dashboards without requiring a calculation there
    agg["on_ground_pct"] = (
        (agg["on_ground_count"] / agg["total_flights"]) * 100
    ).round(2)

    agg["avg_velocity"] = agg["avg_velocity"].round(4)

    log.info(
        "Gold: %d countries aggregated, %d total flights.",
        len(agg),
        int(agg["total_flights"].sum()),
    )

    # Mirror silver timestamp so filenames align across layers
    silver_stem = Path(silver_file).stem               # flights_silver_20260526043004
    ts = silver_stem.replace("flights_silver_", "")    # 20260526043004
    output_path = GOLD_DIR / f"flights_gold_{ts}.csv"

    agg.to_csv(output_path, index=False)

    log.info("Gold file written: %s", output_path)
    context["ti"].xcom_push(key="gold_file", value=str(output_path))
