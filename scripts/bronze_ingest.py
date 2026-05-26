"""
Bronze layer — raw ingestion from OpenSky Network API.

Saves a timestamped, immutable JSON snapshot of all current flight states.
Supports optional basic auth (set OPENSKY_USER / OPENSKY_PASS as Airflow
Variables to increase the anonymous rate limit of 10 req/hr).
"""

import json
import logging
from datetime import datetime, timezone
from pathlib import Path

import requests
from airflow.models import Variable

log = logging.getLogger(__name__)

OPENSKY_URL = "https://opensky-network.org/api/states/all"
BRONZE_DIR = Path("/opt/airflow/data/bronze")

OPENSKY_COLUMNS = [
    "icao24", "callsign", "origin_country", "time_position",
    "last_contact", "longitude", "latitude", "baro_altitude",
    "on_ground", "velocity", "true_track", "vertical_rate",
    "sensors", "geo_altitude", "squawk", "spi", "position_source",
]


def run_bronze_ingestion(**context):
    BRONZE_DIR.mkdir(parents=True, exist_ok=True)

    # Optional auth — raises rate limit from 10/hr (anon) to ~100/hr (registered)
    auth = None
    try:
        user = Variable.get("OPENSKY_USER")
        password = Variable.get("OPENSKY_PASS")
        auth = (user, password)
        log.info("Using authenticated OpenSky request.")
    except Exception:
        log.warning(
            "OPENSKY_USER / OPENSKY_PASS not set — using anonymous access (10 req/hr limit)."
        )

    response = requests.get(OPENSKY_URL, auth=auth, timeout=30)
    response.raise_for_status()

    data = response.json()

    # OpenSky returns {"states": null} when rate-limited or no data available
    if not data or data.get("states") is None:
        raise ValueError(
            "OpenSky API returned no states — likely rate-limited or no flights available. "
            "Airflow will retry automatically."
        )

    log.info("Fetched %d flight states from OpenSky.", len(data["states"]))

    timestamp = datetime.now(timezone.utc).strftime("%Y%m%d%H%M%S")
    output_path = BRONZE_DIR / f"flights_{timestamp}.json"

    with open(output_path, "w") as f:
        json.dump(data, f)

    log.info("Bronze file written: %s", output_path)
    context["ti"].xcom_push(key="bronze_file", value=str(output_path))
