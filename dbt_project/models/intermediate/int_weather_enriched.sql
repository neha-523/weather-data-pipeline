-- Intermediate model: adds derived metrics and categorical flags to staged weather data.
-- Consumed by mart models; not exposed directly to BI tools.

{{ config(materialized='view') }}

with stg as (

    select * from {{ ref('stg_weather_raw') }}

),

enriched as (

    select
        city_name,
        weather_date,
        temperature_max_c,
        temperature_min_c,
        precipitation_mm,
        windspeed_max_kmh,
        ingested_date,

        -- Derived temperature metrics
        round(temperature_max_c - temperature_min_c, 2)              as temp_range_c,
        round((temperature_max_c + temperature_min_c) / 2.0, 2)      as temp_avg_c,

        -- Weather condition flags
        precipitation_mm > 0                                          as is_rainy,
        windspeed_max_kmh > 50                                        as is_windy,

        -- Human-readable temperature band
        case
            when temperature_max_c >= 30 then 'hot'
            when temperature_max_c >= 20 then 'warm'
            when temperature_max_c >= 10 then 'mild'
            else                              'cold'
        end                                                           as temperature_category,

        -- Date dimensions for aggregation
        date_trunc('month', weather_date)                             as weather_month,
        extract('isodow' from weather_date)                           as day_of_week   -- 1=Mon, 7=Sun

    from stg

)

select * from enriched
