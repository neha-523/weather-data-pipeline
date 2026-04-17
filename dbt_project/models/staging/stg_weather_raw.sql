-- Staging model: reads Parquet files directly from MinIO via DuckDB httpfs.
-- One row per city per day, with basic type casting and null filtering.

{{ config(materialized='view') }}

with source as (

    select *
    from read_parquet('s3://processed/weather/**/*.parquet')

),

renamed as (

    select
        city_name,
        cast(weather_date     as date)    as weather_date,
        temperature_max_c                 as temperature_max_c,
        temperature_min_c                 as temperature_min_c,
        precipitation_mm                  as precipitation_mm,
        windspeed_max_kmh                 as windspeed_max_kmh,
        cast(ingested_at      as date)    as ingested_date

    from source
    where city_name    is not null
      and weather_date is not null

),

-- Deduplicate: keep the most recently ingested record per city+date
-- (same weather_date can appear in multiple Parquet files from different DAG runs)
ranked as (

    select
        *,
        row_number() over (
            partition by city_name, weather_date
            order by ingested_date desc
        ) as rn

    from renamed

)

select
    city_name,
    weather_date,
    temperature_max_c,
    temperature_min_c,
    precipitation_mm,
    windspeed_max_kmh,
    ingested_date
from ranked
where rn = 1
