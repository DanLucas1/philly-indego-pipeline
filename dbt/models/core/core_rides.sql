{{
    config(
        materialized='incremental',
        unique_key='trip_id'
    )
}}

WITH rides AS (
    SELECT
        trip_id,
        start_time,
        end_time,
        duration,
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
    -- ride information
    r.trip_id,
    r.start_time,
    r.end_time,
    r.duration,
    r.trip_route_category,
    ST_DISTANCE(
        ST_GEOGPOINT(r.start_lon, r.start_lat),
        ST_GEOGPOINT(r.end_lon, r.end_lat),
        TRUE) * 0.000621371 AS station_distance_miles,

    -- start and end location details
    r.start_lat,
    r.start_lon,
    r.start_station,
    ss.station_name AS start_station_name,
    ss.go_live_date AS start_station_go_live_date,
    ss.station_status AS start_station_status,
    r.start_neighborhood,
    r.end_lat,
    r.end_lon,
    r.end_station,
    se.station_name AS end_station_name,
    se.go_live_date AS end_station_go_live_date,
    se.station_status AS end_station_status,
    r.end_neighborhood,

    -- bike details
    r.bike_id,
    r.bike_type,

    -- rider details
    r.plan_duration,
    r.passholder_type

FROM rides r
LEFT JOIN stations ss ON r.start_station = ss.station_id
LEFT JOIN stations se ON r.end_station = se.station_id