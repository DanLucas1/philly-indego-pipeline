SELECT
    neighborhood_name,
    CAST(shape_leng AS NUMERIC) AS shape_leng,
    CAST(shape_area AS NUMERIC) AS shape_area,
    ST_GEOGFROMTEXT(geometry, FALSE) AS geometry
FROM {{ source('indego_dwh', 'philadelphia_neighborhoods') }}