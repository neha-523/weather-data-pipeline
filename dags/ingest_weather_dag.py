"""
Weather Data Pipeline DAG

Ingests daily weather data for 5 global cities from the Open-Meteo API (free, no key needed).

Flow:
  1. fetch_and_store_raw   — Hits Open-Meteo API, saves raw JSON to MinIO (bronze layer)
  2. transform_to_parquet  — Flattens JSON → Parquet, saves to MinIO (silver layer)
  3. run_dbt               — Runs dbt models + tests to produce mart tables (gold layer)
"""

import json
import logging
from datetime import timedelta
from io import BytesIO

import pandas as pd
import pendulum
import requests
from minio import Minio

from airflow.decorators import dag, task

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

CITIES = [
    {"name": "New York", "lat": 40.7128, "lon": -74.0060},
    {"name": "London",   "lat": 51.5074, "lon": -0.1278},
    {"name": "Tokyo",    "lat": 35.6762, "lon": 139.6503},
    {"name": "Sydney",   "lat": -33.8688, "lon": 151.2093},
    {"name": "Mumbai",   "lat": 19.0760,  "lon": 72.8777},
]

MINIO_ENDPOINT  = "minio:9000"
MINIO_ACCESS_KEY = "minioadmin"
MINIO_SECRET_KEY = "minioadmin"
RAW_BUCKET       = "raw"
PROCESSED_BUCKET = "processed"

OPEN_METEO_URL = (
    "https://api.open-meteo.com/v1/forecast"
    "?latitude={lat}&longitude={lon}"
    "&daily=temperature_2m_max,temperature_2m_min,precipitation_sum,windspeed_10m_max"
    "&past_days=1&forecast_days=1&timezone=auto"
)


def _minio_client() -> Minio:
    return Minio(MINIO_ENDPOINT, access_key=MINIO_ACCESS_KEY, secret_key=MINIO_SECRET_KEY, secure=False)


def _ensure_bucket(client: Minio, bucket: str) -> None:
    if not client.bucket_exists(bucket):
        client.make_bucket(bucket)
        logger.info("Created bucket: %s", bucket)


# ---------------------------------------------------------------------------
# DAG definition
# ---------------------------------------------------------------------------

@dag(
    dag_id="weather_data_pipeline",
    schedule="@daily",
    start_date=pendulum.datetime(2024, 1, 1, tz="UTC"),
    catchup=False,
    default_args={
        "retries": 3,
        "retry_delay": timedelta(minutes=5),
    },
    tags=["weather", "ingestion", "dbt"],
    doc_md=__doc__,
)
def weather_data_pipeline():

    @task()
    def fetch_and_store_raw(ds=None) -> dict:
        """
        Fetch yesterday's weather for each city and store raw JSON
        in MinIO at: raw/weather/YYYY/MM/DD/<city>.json
        """
        client = _minio_client()
        _ensure_bucket(client, RAW_BUCKET)

        year, month, day = ds.split("-")
        stored_files = []

        for city in CITIES:
            url = OPEN_METEO_URL.format(lat=city["lat"], lon=city["lon"])
            response = requests.get(url, timeout=30)
            response.raise_for_status()

            payload = response.json()
            payload["city_name"] = city["name"]

            city_slug = city["name"].replace(" ", "_").lower()
            object_key = f"weather/{year}/{month}/{day}/{city_slug}.json"
            raw_bytes = json.dumps(payload).encode("utf-8")

            client.put_object(
                RAW_BUCKET,
                object_key,
                BytesIO(raw_bytes),
                length=len(raw_bytes),
                content_type="application/json",
            )
            logger.info("Stored raw JSON: %s/%s", RAW_BUCKET, object_key)
            stored_files.append(object_key)

        return {"execution_date": ds, "files": stored_files}

    @task()
    def transform_to_parquet(raw_result: dict) -> str:
        """
        Read raw JSON files from MinIO, flatten to tabular format,
        and write a single Parquet file to: processed/weather/YYYY/MM/DD/weather_data.parquet
        """
        client = _minio_client()
        _ensure_bucket(client, PROCESSED_BUCKET)

        execution_date = raw_result["execution_date"]
        year, month, day = execution_date.split("-")
        records = []

        for object_key in raw_result["files"]:
            response = client.get_object(RAW_BUCKET, object_key)
            data = json.loads(response.read())
            daily = data["daily"]

            for i, date in enumerate(daily["time"]):
                records.append({
                    "city_name":          data["city_name"],
                    "weather_date":       date,
                    "temperature_max_c":  daily["temperature_2m_max"][i],
                    "temperature_min_c":  daily["temperature_2m_min"][i],
                    "precipitation_mm":   daily["precipitation_sum"][i],
                    "windspeed_max_kmh":  daily["windspeed_10m_max"][i],
                    "ingested_at":        execution_date,
                })

        df = pd.DataFrame(records)
        logger.info("Flattened %d rows for %s", len(df), execution_date)

        parquet_key = f"weather/{year}/{month}/{day}/weather_data.parquet"
        buffer = BytesIO()
        df.to_parquet(buffer, engine="pyarrow", index=False)
        buffer.seek(0)

        client.put_object(
            PROCESSED_BUCKET,
            parquet_key,
            buffer,
            length=buffer.getbuffer().nbytes,
            content_type="application/octet-stream",
        )
        logger.info("Stored Parquet: %s/%s", PROCESSED_BUCKET, parquet_key)
        return parquet_key

    @task()
    def run_dbt(_parquet_key: str) -> None:
        """Run dbt transformations and tests after new Parquet data is available."""
        import subprocess
        result = subprocess.run(
            "cd /opt/airflow/dbt_project && dbt deps && dbt run && dbt test",
            shell=True,
            capture_output=True,
            text=True,
        )
        logger.info(result.stdout)
        if result.returncode != 0:
            raise Exception(f"dbt failed:\n{result.stderr}")

    # Pipeline wiring
    raw     = fetch_and_store_raw()
    parquet = transform_to_parquet(raw)
    run_dbt(parquet)


weather_data_pipeline()
