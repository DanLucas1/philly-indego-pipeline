-- stg_rides.sql

{{ config(materialized='view') }}

select * from {{ source('staging', 'fact_indego_rides') }}
limit 100