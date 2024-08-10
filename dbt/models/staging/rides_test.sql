
SELECT
COUNT (DISTINCT trip_id) AS trip_total
FROM `indego-pipeline.indego_tripdata.fact_indego_rides`
WHERE start_time > '2024-01-01'