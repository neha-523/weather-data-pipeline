import duckdb

con = duckdb.connect("data/warehouse.duckdb")

con.execute("LOAD httpfs")
con.execute("SET s3_endpoint='localhost:9000'")
con.execute("SET s3_access_key_id='minioadmin'")
con.execute("SET s3_secret_access_key='minioadmin'")
con.execute("SET s3_use_ssl=false")
con.execute("SET s3_url_style='path'")

print("\n========== STAGE 1: Staging (deduplicated) ==========")
df = con.execute("""
    SELECT city_name, weather_date, temperature_max_c, temperature_min_c,
           precipitation_mm, windspeed_max_kmh, ingested_date
    FROM main_staging.stg_weather_raw
    ORDER BY weather_date, city_name
""").df()
print(df.to_string(index=False))
print(f"\nTotal rows: {len(df)}  |  Unique (city, date) pairs: {df[['city_name','weather_date']].drop_duplicates().shape[0]}")

print("\n========== STAGE 2: Intermediate (enriched with derived columns) ==========")
df2 = con.execute("""
    SELECT city_name, weather_date, temp_avg_c, temp_range_c,
           temperature_category, is_rainy, is_windy
    FROM main_intermediate.int_weather_enriched
    ORDER BY weather_date, city_name
""").df()
print(df2.to_string(index=False))

print("\n========== STAGE 3: Mart (monthly aggregates per city) ==========")
df3 = con.execute("""
    SELECT city_name, weather_month, avg_temp_c, record_high_c, record_low_c,
           total_precipitation_mm, rainy_days, total_days
    FROM main_marts.mart_city_weather_summary
    ORDER BY city_name, weather_month
""").df()
print(df3.to_string(index=False))
