CREATE DATABASE IF NOT EXISTS flights;
CREATE SCHEMA  IF NOT EXISTS flights.kpi;

CREATE TABLE IF NOT EXISTS flights.kpi.flight_kpi (
    window_start        TIMESTAMP_NTZ   NOT NULL,   -- FIX: explicit no-timezone; avoids session-default surprises
    origin_country      VARCHAR(100)    NOT NULL,   -- FIX: bounded type instead of TEXT (16MB alias)
    total_flights       INTEGER         NOT NULL,
    avg_velocity        FLOAT           NOT NULL,
    on_ground_count     INTEGER         NOT NULL,   -- FIX: renamed from on_ground (was ambiguous — looks like bool)
    on_ground_pct       FLOAT,                      -- NEW: derived metric, computed in gold layer
    load_time           TIMESTAMP_NTZ   DEFAULT CURRENT_TIMESTAMP(),

    PRIMARY KEY (window_start, origin_country)
)
CLUSTER BY (window_start)   -- NEW: time-range queries scan only relevant micro-partitions
COMMENT = 'Gold KPI table — 30-min flight activity aggregated by country. Source: OpenSky Network.';
