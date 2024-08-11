{{config(materialized='view')}}

SELECT
    {{ dbt.safe_cast("trip_id", api.Column.translate_type("integer")) }} AS trip_id,
    {{ dbt.safe_cast("duration", api.Column.translate_type("integer")) }} AS duration,
    cast(start_time AS TIMESTAMP) AS start_time,
    cast(end_time AS TIMESTAMP) AS end_time,
    {{ dbt.safe_cast("start_station", api.Column.translate_type("integer")) }} AS start_station,
    cast(ABS(start_lat) AS NUMERIC) AS start_lat,
    cast(-1 * ABS(start_lon) AS NUMERIC) AS start_lon,
    {{ dbt.safe_cast("end_station", api.Column.translate_type("integer")) }} AS end_station,
    cast(ABS(end_lat) AS NUMERIC) AS end_lat,
    cast(-1 * ABS(end_lon) AS NUMERIC) AS end_lon,
    {{ dbt.safe_cast("bike_id", api.Column.translate_type("integer")) }} AS bike_id,
    {{ dbt.safe_cast("plan_duration", api.Column.translate_type("integer")) }} AS plan_duration,
    trip_route_category,
    passholder_type,
    COALESCE(bike_type, 'unknown') AS bike_type,
    COALESCE(start_neighborhood, 'Unknown') AS start_neighborhood,      
    COALESCE(end_neighborhood, 'Unknown') AS end_neighborhood
FROM {{ source('indego_dwh', 'fact_indego_rides') }}
WHERE end_time >= start_time

{% if var('is_test_run', default=true) %}
    limit 100
{% endif %}