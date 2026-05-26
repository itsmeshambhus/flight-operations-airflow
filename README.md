# ✈️ Flight Operations — Airflow Medallion Pipeline

A **production-style data engineering pipeline** that pulls live global flight data from the [OpenSky Network](https://opensky-network.org/) API every 30 minutes, processes it through a **Medallion Architecture** (Bronze → Silver → Gold), and loads aggregated KPIs into **Snowflake** — ready for BI dashboards.

---

## Architecture overview

```
OpenSky Network API
        │
        ▼
┌─────────────────────────────────────────────────────────┐
│                 Apache Airflow (Docker)                  │
│                                                          │
│  bronze_ingest → silver_transform → gold_aggregate       │
│                                          │               │
│                                 load_gold_to_snowflake   │
└─────────────────────────────────────────────────────────┘
        │
        ▼
  Snowflake — flights.kpi.flight_kpi
```

| Layer  | Storage path                        | Format | Contents                              |
|--------|-------------------------------------|--------|---------------------------------------|
| Bronze | `data/bronze/flights_<ts>.json`     | JSON   | Raw API snapshot — immutable          |
| Silver | `data/silver/flights_silver_<ts>.csv` | CSV  | Cleaned, typed, validated             |
| Gold   | `data/gold/flights_gold_<ts>.csv`   | CSV    | KPIs per country per 30-min window    |

---

## Tech stack

| Tool | Role |
|------|------|
| **Apache Airflow 2.9.3** | Pipeline orchestration & scheduling |
| **Python 3.12** | Transformation & aggregation logic |
| **OpenSky Network REST API** | Live flight data source |
| **Snowflake** | Cloud data warehouse (Gold layer) |
| **Docker & Docker Compose** | Local environment |
| **pandas / numpy** | Data processing |

---

## Project structure

```
flight-operations-airflow/
├── dags/
│   └── flight_pipeline.py          # Airflow DAG — task definitions & dependencies
├── scripts/
│   ├── bronze_ingest.py            # Raw API fetch → timestamped JSON
│   ├── silver_transform.py         # Clean, type, validate → CSV
│   ├── gold_aggregate.py           # Aggregate KPIs by country → CSV
│   └── load_gold_to_snowflake.py   # MERGE into Snowflake
├── sql/
│   └── create_table.sql            # Snowflake DDL — run once before pipeline
├── data/
│   ├── bronze/                     # Raw JSON snapshots (git-ignored)
│   ├── silver/                     # Cleaned CSVs (git-ignored)
│   └── gold/                       # Aggregated KPIs (git-ignored)
├── logs/                           # Airflow task logs (git-ignored)
├── plugins/                        # Airflow custom plugins (empty)
├── docker-compose.yml              # Postgres + Airflow services
├── requirements.txt                # Python dependencies
├── .env.example                    # Environment variable template
└── .gitignore
```

---

## Quickstart

### Prerequisites
- [Docker Desktop](https://www.docker.com/products/docker-desktop/) installed and running
- A free [Snowflake](https://signup.snowflake.com/) account
- A free [OpenSky Network](https://opensky-network.org/login) account (optional — raises API rate limit)

---

### 1. Clone the repo

```bash
git clone https://github.com/<your-username>/flight-operations-airflow.git
cd flight-operations-airflow
```

### 2. Configure environment variables

```bash
cp .env.example .env
```

Edit `.env` with your values — at minimum change the passwords:

```env
POSTGRES_USER=airflow
POSTGRES_PASSWORD=your_secure_password
POSTGRES_DB=airflow_db

AIRFLOW_ADMIN_USER=admin
AIRFLOW_ADMIN_PASSWORD=your_secure_password
AIRFLOW_ADMIN_EMAIL=you@example.com
```

### 3. Create the Snowflake table

Run [`sql/create_table.sql`](sql/create_table.sql) in your Snowflake worksheet:

```sql
CREATE DATABASE IF NOT EXISTS flights;
CREATE SCHEMA IF NOT EXISTS flights.kpi;

CREATE TABLE IF NOT EXISTS flights.kpi.flight_kpi (
    window_start     TIMESTAMP_NTZ  NOT NULL,
    origin_country   VARCHAR(100)   NOT NULL,
    total_flights    INTEGER        NOT NULL,
    avg_velocity     FLOAT          NOT NULL,
    on_ground_count  INTEGER        NOT NULL,
    on_ground_pct    FLOAT,
    load_time        TIMESTAMP_NTZ  DEFAULT CURRENT_TIMESTAMP(),
    PRIMARY KEY (window_start, origin_country)
)
CLUSTER BY (window_start);
```

### 4. Start Airflow

```bash
# First run — initialise the database and create the admin user
docker compose up airflow-init

# Then start all services
docker compose up -d
```

Airflow UI → [http://localhost:8080](http://localhost:8080)
Login with the credentials you set in `.env`.

### 5. Add the Snowflake connection in Airflow

Go to **Admin → Connections → + Add** and fill in:

| Field | Value |
|-------|-------|
| Connection ID | `flight_snowflake` |
| Connection Type | `Snowflake` |
| Login | your Snowflake username |
| Password | your Snowflake password |
| Schema | `kpi` |
| Extra (JSON) | `{"account": "your-account-id", "warehouse": "COMPUTE_WH", "database": "flights", "role": "ACCOUNTADMIN"}` |

> Your Snowflake account ID looks like `abc12345.us-east-1` — find it under **Admin → Accounts**.

### 6. (Optional) Add OpenSky credentials

Anonymous OpenSky access is limited to **10 requests per hour**. To raise this limit, register at [opensky-network.org](https://opensky-network.org/login), then add your credentials as Airflow Variables:

Go to **Admin → Variables → + Add**:

| Key | Value |
|-----|-------|
| `OPENSKY_USER` | your OpenSky username |
| `OPENSKY_PASS` | your OpenSky password |

### 7. Trigger the DAG

In the Airflow UI, enable the `flights_ops_medallion_pipe` DAG. It runs automatically every 30 minutes, or you can trigger it manually with the ▶ button.

---

## DAG design

```
bronze_ingest ──► silver_transform ──► gold_aggregate ──► load_gold_to_snowflake
```

| Task | What it does |
|------|-------------|
| `bronze_ingest` | GETs the OpenSky `/states/all` endpoint, validates the response is non-null, saves a timestamped JSON file |
| `silver_transform` | Reads bronze JSON, maps columns **by name** (not position), casts types, drops nulls, writes a per-window CSV |
| `gold_aggregate` | Groups by `origin_country`, computes `total_flights`, `avg_velocity`, `on_ground_count`, `on_ground_pct` |
| `load_gold_to_snowflake` | Connects via Airflow BaseHook, runs a MERGE upsert using `executemany()` for efficiency |

**Retry policy**: 3 retries with a 5-minute delay — handles transient API failures and network blips automatically.

---

## Snowflake schema

```sql
flights.kpi.flight_kpi
├── window_start     TIMESTAMP_NTZ   PK  -- start of the 30-min collection window (UTC)
├── origin_country   VARCHAR(100)    PK  -- country of registration
├── total_flights    INTEGER             -- total aircraft observed
├── avg_velocity     FLOAT               -- mean velocity in m/s
├── on_ground_count  INTEGER             -- aircraft on the ground
├── on_ground_pct    FLOAT               -- % of fleet on the ground
└── load_time        TIMESTAMP_NTZ       -- when this row was last written
```

Clustered by `window_start` for fast time-range queries.
MERGE logic is fully **idempotent** — re-running a window is safe.

---

## What each fix addressed

| Issue | Fix applied |
|-------|-------------|
| `retries=0` | Set to `retries=3` with 5-min delay |
| Column assignment by position | Renamed columns using index→name dict — safe against API schema changes |
| `data["states"]` null not checked | Explicit null check with a descriptive error in both bronze and silver |
| Bronze dir not created | `mkdir(parents=True, exist_ok=True)` in `bronze_ingest` |
| Silver overwrote same daily file | Now uses `data_interval_start` timestamp — every window gets its own file |
| `on_ground` bool read as object | Cast to `bool` then `int` before aggregation |
| `avg_velocity` included NaN | `pd.to_numeric(errors="coerce")` — pandas `mean()` skips NaN by default |
| Per-row `cursor.execute()` loop | Replaced with `executemany()` — one round-trip for all country rows |
| NaN guard before `float()` | `fillna(0.0)` on velocity and on_ground before building tuple list |
| `load_time` missing on INSERT | Added to INSERT column list in MERGE SQL |
| `gold_path` fragile string replace | Derives timestamp from silver filename stem — robust to path changes |
| No DAG tags or doc | Added `tags`, `doc_md`, and per-task `doc_md` |
| `TEXT` type in Snowflake | DDL uses `VARCHAR(100)` |
| `TIMESTAMP` ambiguous type | DDL uses `TIMESTAMP_NTZ` |
| `on_ground` misleading column name | Renamed to `on_ground_count`; added `on_ground_pct` |
| No `CLUSTER BY` | Added `CLUSTER BY (window_start)` for query performance |

---

## Sample gold output

| origin_country | total_flights | avg_velocity | on_ground_count | on_ground_pct |
|----------------|---------------|--------------|-----------------|---------------|
| United States  | 5,842         | 218.4        | 1,203           | 20.59         |
| Germany        | 1,104         | 196.2        | 287             | 26.00         |
| United Kingdom | 982           | 204.7        | 198             | 20.16         |

---

## Future improvements

- [ ] Add [Great Expectations](https://greatexpectations.io/) data quality checks on the silver layer
- [ ] Introduce a `dbt` project for Silver → Gold transformations
- [ ] Add Slack/email alerting on DAG failure
- [ ] Build a Power BI dashboard on top of `flight_kpi`
- [ ] Containerise with a proper Airflow image that pre-installs `requirements.txt`

---

## License

MIT
