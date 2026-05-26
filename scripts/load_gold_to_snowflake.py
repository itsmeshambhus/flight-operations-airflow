"""
Snowflake load — MERGE gold KPIs into FLIGHT_KPI table.

Uses executemany() instead of a per-row execute() loop to reduce round-trips
from ~200 to 1 per DAG run. The MERGE is fully idempotent: re-running any
window produces the same result.

Airflow connection ID: flight_snowflake
Required extras: account, warehouse, database, role
"""

import logging

import pandas as pd
import snowflake.connector
from airflow.hooks.base import BaseHook

log = logging.getLogger(__name__)

MERGE_SQL = """
    MERGE INTO FLIGHT_KPI tgt
    USING (
        SELECT
            TO_TIMESTAMP_NTZ(%s)  AS WINDOW_START,
            %s                    AS ORIGIN_COUNTRY,
            %s                    AS TOTAL_FLIGHTS,
            %s                    AS AVG_VELOCITY,
            %s                    AS ON_GROUND_COUNT,
            %s                    AS ON_GROUND_PCT
    ) src
    ON  tgt.WINDOW_START    = src.WINDOW_START
    AND tgt.ORIGIN_COUNTRY  = src.ORIGIN_COUNTRY
    WHEN MATCHED THEN UPDATE SET
        TOTAL_FLIGHTS   = src.TOTAL_FLIGHTS,
        AVG_VELOCITY    = src.AVG_VELOCITY,
        ON_GROUND_COUNT = src.ON_GROUND_COUNT,
        ON_GROUND_PCT   = src.ON_GROUND_PCT,
        LOAD_TIME       = CURRENT_TIMESTAMP()
    WHEN NOT MATCHED THEN INSERT
        (WINDOW_START, ORIGIN_COUNTRY, TOTAL_FLIGHTS, AVG_VELOCITY, ON_GROUND_COUNT, ON_GROUND_PCT, LOAD_TIME)
    VALUES
        (src.WINDOW_START, src.ORIGIN_COUNTRY, src.TOTAL_FLIGHTS, src.AVG_VELOCITY,
         src.ON_GROUND_COUNT, src.ON_GROUND_PCT, CURRENT_TIMESTAMP());
"""


def _get_snowflake_connection():
    conn = BaseHook.get_connection("flight_snowflake")
    return snowflake.connector.connect(
        user=conn.login,
        password=conn.password,
        account=conn.extra_dejson["account"],
        warehouse=conn.extra_dejson.get("warehouse"),
        database=conn.extra_dejson.get("database"),
        schema=conn.schema,
        role=conn.extra_dejson.get("role"),
    )


def load_gold_to_snowflake(**context):
    gold_file = context["ti"].xcom_pull(key="gold_file", task_ids="gold_aggregate")
    if not gold_file:
        raise ValueError("Gold file path not found in XCom.")

    window_start = context["data_interval_start"].strftime("%Y-%m-%d %H:%M:%S")

    df = pd.read_csv(gold_file)
    if df.empty:
        raise ValueError(f"Gold file is empty: {gold_file}")

    # Guard against NaN values that would throw inside float()
    df["avg_velocity"] = pd.to_numeric(df["avg_velocity"], errors="coerce").fillna(0.0)
    df["on_ground_count"] = pd.to_numeric(df["on_ground_count"], errors="coerce").fillna(0).astype(int)
    df["on_ground_pct"] = pd.to_numeric(df.get("on_ground_pct", 0), errors="coerce").fillna(0.0)

    # Build list of tuples — one per country row
    rows = [
        (
            window_start,
            str(row["origin_country"]),
            int(row["total_flights"]),
            float(row["avg_velocity"]),
            int(row["on_ground_count"]),
            float(row["on_ground_pct"]),
        )
        for _, row in df.iterrows()
    ]

    log.info("Loading %d rows for window_start=%s into Snowflake.", len(rows), window_start)

    sf_conn = _get_snowflake_connection()
    try:
        with sf_conn.cursor() as cursor:
            # executemany sends all rows in one round-trip instead of N round-trips
            cursor.executemany(MERGE_SQL, rows)
        sf_conn.commit()
        log.info("Snowflake MERGE complete — %d rows upserted.", len(rows))
    finally:
        sf_conn.close()
