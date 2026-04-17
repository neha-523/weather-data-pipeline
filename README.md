# Weather Data Lakehouse Pipeline

End-to-end data engineering pipeline that ingests daily weather data for 5 global cities, stores it in a local data lake, and serves analytics-ready tables through dbt transformations.

## Architecture

```
Open-Meteo API (free, no key)
       │
       ▼
 Apache Airflow (MWAA-equivalent)
  ├── fetch_and_store_raw     → MinIO /raw/       (bronze: raw JSON)
  ├── transform_to_parquet    → MinIO /processed/ (silver: Parquet)
  └── run_dbt
           │
           ▼
      dbt Core + DuckDB
       ├── staging/     → stg_weather_raw         (typed, cleaned)
       ├── intermediate/→ int_weather_enriched     (derived metrics + flags)
       └── marts/       → mart_city_weather_summary (monthly aggregates)
```

**Cities tracked:** New York · London · Tokyo · Sydney · Mumbai

## Tech Stack

| Layer | Tool | Cloud Equivalent |
|---|---|---|
| Orchestration | Apache Airflow 2.8 | AWS MWAA |
| Object Storage | MinIO | AWS S3 |
| Transformation | dbt Core + DuckDB | AWS Glue + Athena |
| Containerisation | Docker Compose | — |

## Project Structure

```
├── dags/
│   └── ingest_weather_dag.py     # Airflow DAG (TaskFlow API)
├── dbt_project/
│   ├── models/
│   │   ├── staging/              # Raw → typed views
│   │   ├── intermediate/         # Business logic layer
│   │   └── marts/                # Aggregated tables for BI
│   ├── dbt_project.yml
│   └── profiles.yml              # DuckDB + MinIO/S3 config
├── scripts/
│   └── init_minio_buckets.py     # One-time bucket setup
├── docker-compose.yml
├── Dockerfile
└── requirements.txt
```

## Quick Start

### Prerequisites
- Docker + Docker Compose
- Python 3.11+

### 1. Clone & configure

```bash
git clone <repo-url>
cd DE_Project
cp .env.example .env
```

### 2. Start all services

```bash
docker compose up --build -d
```

Services started:
- **Airflow UI** → http://localhost:8080 (admin / admin)
- **MinIO Console** → http://localhost:9001 (minioadmin / minioadmin)

### 3. Initialise MinIO buckets

```bash
pip install minio
python scripts/init_minio_buckets.py
```

### 4. Trigger the pipeline

Go to http://localhost:8080 → enable the `weather_data_pipeline` DAG → trigger a manual run.

The DAG will:
1. Fetch weather data from Open-Meteo API for all 5 cities
2. Store raw JSON in MinIO (`raw/` bucket)
3. Convert to Parquet and store in MinIO (`processed/` bucket)
4. Run dbt models to build the `mart_city_weather_summary` table in DuckDB

### 5. Query results

```python
import duckdb

con = duckdb.connect("data/warehouse.duckdb")
df = con.execute("SELECT * FROM marts.mart_city_weather_summary ORDER BY weather_month").df()
print(df)
```

## dbt Data Models

### Staging — `stg_weather_raw`
Reads Parquet directly from MinIO using DuckDB's `httpfs` extension. Applies type casting and null filters.

### Intermediate — `int_weather_enriched`
Adds derived columns:
- `temp_range_c` — daily temperature swing
- `temp_avg_c` — daily mean temperature
- `is_rainy` / `is_windy` — boolean flags
- `temperature_category` — hot / warm / mild / cold

### Mart — `mart_city_weather_summary`
Monthly aggregates per city: avg/min/max temperatures, total precipitation, rainy days, windy days, temperature category breakdown.

## Data Quality

dbt tests run automatically after every pipeline execution:
- `not_null` checks on key columns
- `accepted_values` on `city_name`
- Row-level completeness via `total_days`

## Stopping Services

```bash
docker compose down          # stop containers
docker compose down -v       # stop + delete volumes (full reset)
```
