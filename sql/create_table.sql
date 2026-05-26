-- Flight KPI table — Gold layer destination in Snowflake
-- Run once before starting the pipeline.

CREATE DATABASE IF NOT EXISTS flights;
CREATE SCHEMA IF NOT EXISTS flights.kpi;

CREATE TABLE IF NOT EXISTS flights.kpi.flight_kpi (
    window_start        TIMESTAMP_NTZ   NOT NULL,
    origin_country      VARCHAR(100)    NOT NULL,
    total_flights       INTEGER         NOT NULL,
    avg_velocity        FLOAT           NOT NULL,
    on_ground_count     INTEGER         NOT NULL,
    on_ground_pct       FLOAT,
    load_time           TIMESTAMP_NTZ   DEFAULT CURRENT_TIMESTAMP(),

    PRIMARY KEY (window_start, origin_country)
)
CLUSTER BY (window_start)
COMMENT = 'Gold KPI table — 30-min flight activity aggregated by country. Source: OpenSky Network.';
