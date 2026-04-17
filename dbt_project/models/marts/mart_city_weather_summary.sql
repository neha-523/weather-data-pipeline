-- Mart model: monthly weather summary per city, ready for BI consumption.
-- Materialised as a table so dashboards query pre-aggregated data.

{{ config(materialized='table') }}

with enriched as (

    select * from {{ ref('int_weather_enriched') }}

),

monthly as (

    select
        city_name,
        weather_month,

        -- Temperature aggregates
        round(avg(temperature_max_c), 2)                        as avg_temp_max_c,
        round(avg(temperature_min_c), 2)                        as avg_temp_min_c,
        round(avg(temp_avg_c), 2)                               as avg_temp_c,
        round(max(temperature_max_c), 2)                        as record_high_c,
        round(min(temperature_min_c), 2)                        as record_low_c,
        round(avg(temp_range_c), 2)                             as avg_temp_range_c,

        -- Precipitation
        round(sum(precipitation_mm), 2)                         as total_precipitation_mm,
        count(*) filter (where is_rainy)                        as rainy_days,

        -- Wind
        round(avg(windspeed_max_kmh), 2)                        as avg_windspeed_kmh,
        round(max(windspeed_max_kmh), 2)                        as max_windspeed_kmh,
        count(*) filter (where is_windy)                        as windy_days,

        -- Temperature category breakdown
        count(*) filter (where temperature_category = 'hot')   as hot_days,
        count(*) filter (where temperature_category = 'warm')  as warm_days,
        count(*) filter (where temperature_category = 'mild')  as mild_days,
        count(*) filter (where temperature_category = 'cold')  as cold_days,

        count(*)                                                as total_days

    from enriched
    group by city_name, weather_month

)

select * from monthly
order by city_name, weather_month
