{{
    config(
        materialized='incremental',
        unique_key='trip_id'
    )
}}

WITH rides AS (
    SELECT
        trip_id,
        duration,
        start_time,
        end_time,
        start_station,
        start_lat,
        start_lon,
        end_station,
        end_lat,
        end_lon,
        bike_id,
        plan_duration,
        trip_route_category,
        passholder_type,
        bike_type,
        start_neighborhood,
        end_neighborhood
    FROM {{ ref('stg_rides') }}
    {% if var('is_test_run', default=true) %}
        limit 100
    {% endif %}
),

stations AS (
    SELECT
        station_id,
        station_name,
        go_live_date,
        station_status
    FROM {{ ref('stations') }})

SELECT
    r.trip_id,
    r.duration,
    r.start_time,
    r.end_time,
    r.start_station,
    r.start_lat,
    r.start_lon,
    r.end_station,
    r.end_lat,
    r.end_lon,
    r.bike_id,
    r.plan_duration,
    r.trip_route_category,
    r.passholder_type,
    r.bike_type,
    r.start_neighborhood,
    r.end_neighborhood,
    ss.station_name AS start_station_name,
    ss.go_live_date AS start_station_go_live_date,
    ss.station_status AS start_station_status,
    se.station_name AS end_station_name,
    se.go_live_date AS end_station_go_live_date,
    se.station_status AS end_station_status,
FROM rides r
LEFT JOIN stations ss ON r.start_station = ss.station_id
LEFT JOIN stations se ON r.end_station = se.station_id