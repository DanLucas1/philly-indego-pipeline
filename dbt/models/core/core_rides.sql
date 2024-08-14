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
        ST_GEOGPOINT(start_lon, start_lat) AS start_point,
        end_station,
        end_lat,
        end_lon,
        ST_GEOGPOINT(end_lon, end_lat) AS end_point,
        bike_id,
        plan_duration,
        trip_route_category,
        passholder_type,
        bike_type
    FROM {{ ref('stg_fact_rides') }}
    {% if var('is_test_run', default=true) %}
        limit 100
    {% endif %}
),

stations AS (
    SELECT
        station_id,
        station_name,
        go_live_date,
        status
    FROM {{ ref('stg_dim_stations') }}),

neighborhoods AS (
    SELECT
        neighborhood_name,
        shape_leng,
        shape_area,
        geometry
    FROM {{ ref('stg_dim_neighborhoods') }}
)

SELECT
    -- ride information
    r.trip_id,
    r.start_time,
    r.end_time,
    r.duration AS duration_minutes,
    r.trip_route_category AS trip_type,
    ST_DISTANCE(r.start_point, r.end_point, TRUE) * 0.000621371 AS station_distance_miles,

    -- start and end location details
    r.start_lat,
    r.start_lon,
    r.start_station AS start_station_id,
    ss.station_name AS start_station_name,
    ss.go_live_date AS start_station_go_live_date,
    ss.status AS start_station_status,
    ns.neighborhood_name AS start_neighborhood,

    r.end_lat,
    r.end_lon,
    r.end_station AS end_station_id,
    se.station_name AS end_station_name,
    se.go_live_date AS end_station_go_live_date,
    se.status AS end_station_status,
    ne.neighborhood_name AS end_neighborhood,

    -- bike details
    r.bike_id,
    r.bike_type,

    -- rider details
    r.plan_duration,
    r.passholder_type

FROM rides r
LEFT JOIN stations ss ON r.start_station = ss.station_id
LEFT JOIN stations se ON r.end_station = se.station_id
LEFT JOIN neighborhoods ns ON ST_WITHIN(r.start_point, ns.geometry)
LEFT JOIN neighborhoods ne ON ST_WITHIN(r.end_point, ne.geometry)