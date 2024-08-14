
SELECT
    {{ dbt.safe_cast("station_id", api.Column.translate_type("integer")) }} AS station_id,
    station_name,
    cast(go_live_date AS TIMESTAMP) AS go_live_date,
    status
FROM {{ source('indego_dwh', 'indego_stations') }}
WHERE station_id IS NOT NULL
