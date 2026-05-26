import sys
from pathlib import Path
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator

AIRFLOW_HOME = Path("/opt/airflow")

if str(AIRFLOW_HOME) not in sys.path:
    sys.path.insert(0, str(AIRFLOW_HOME))

from scripts.bronze_ingest import run_bronze_ingestion
from scripts.silver_transform import run_silver_transform
from scripts.gold_aggregate import run_gold_aggregate
from scripts.load_gold_to_snowflake import load_gold_to_snowflake

default_args = {
    "owner": "airflow",
    "retries": 0,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": False,
    "email_on_retry": False,
}

doc_md = """
## Flight Operations Medallion Pipeline

Pulls live global flight data from the OpenSky Network API every 30 minutes
and processes it through a Medallion Architecture (Bronze → Silver → Gold)
before loading KPIs into Snowflake.

### Task flow
`bronze_ingest` → `silver_transform` → `gold_aggregate` → `load_gold_to_snowflake`

### Layers
| Layer  | Location                            | Format | Description                        |
|--------|-------------------------------------|--------|------------------------------------|
| Bronze | data/bronze/flights_<ts>.json       | JSON   | Raw API response, immutable        |
| Silver | data/silver/flights_silver_<ts>.csv | CSV    | Cleaned, typed, validated          |
| Gold   | data/gold/flights_gold_<ts>.csv     | CSV    | Aggregated KPIs per country/window |

### External dependencies
- **OpenSky Network API**: https://opensky-network.org/api/states/all
- **Snowflake connection**: Airflow connection ID `flight_snowflake`
"""

with DAG(
    dag_id="flights_ops_medallion_pipe",
    default_args=default_args,
    description="Live flight data pipeline: OpenSky → Bronze → Silver → Gold → Snowflake",
    start_date=datetime(2026, 5, 10),
    schedule_interval="*/30 * * * *",
    catchup=False,
    tags=["flights", "medallion", "snowflake", "opensky"],
    doc_md=doc_md,
) as dag:

    bronze = PythonOperator(
        task_id="bronze_ingest",
        python_callable=run_bronze_ingestion,
        doc_md="Fetch raw flight states from OpenSky API and save timestamped JSON to bronze layer.",
    )

    silver = PythonOperator(
        task_id="silver_transform",
        python_callable=run_silver_transform,
        doc_md="Parse bronze JSON, assign columns by name, validate, clean nulls, write to silver CSV.",
    )

    gold = PythonOperator(
        task_id="gold_aggregate",
        python_callable=run_gold_aggregate,
        doc_md="Aggregate silver data by country: total flights, avg velocity, on-ground count.",
    )

    load_to_snowflake = PythonOperator(
        task_id="load_gold_to_snowflake",
        python_callable=load_gold_to_snowflake,
        doc_md="MERGE gold KPIs into Snowflake FLIGHT_KPI table using executemany for efficiency.",
    )

    bronze >> silver >> gold >> load_to_snowflake
