import pandas as pd
import pyarrow as pa
from google.cloud import bigquery

# BigQuery schema
bq_schema = [
    bigquery.SchemaField('trip_id', 'INTEGER'),
    bigquery.SchemaField('duration', 'INTEGER'),
    bigquery.SchemaField('start_time', 'TIMESTAMP'),
    bigquery.SchemaField('end_time', 'TIMESTAMP'),
    bigquery.SchemaField('start_station', 'INTEGER'),
    bigquery.SchemaField('start_lat', 'FLOAT'),
    bigquery.SchemaField('start_lon', 'FLOAT'),
    bigquery.SchemaField('end_station', 'INTEGER'),
    bigquery.SchemaField('end_lat', 'FLOAT'),
    bigquery.SchemaField('end_lon', 'FLOAT'),
    bigquery.SchemaField('bike_id', 'FLOAT'),
    bigquery.SchemaField('plan_duration', 'INTEGER'),
    bigquery.SchemaField('trip_route_category', 'STRING'),
    bigquery.SchemaField('passholder_type', 'STRING'),
    bigquery.SchemaField('bike_type', 'STRING')
]

# Pyarrow schema
arrow_schema = pa.schema([
    ('trip_id', pa.int64()),
    ('duration', pa.int64()),
    ('start_time', pa.timestamp('ns')),
    ('end_time', pa.timestamp('ns')),
    ('start_station', pa.int64()),
    ('start_lat', pa.float64()),
    ('start_lon', pa.float64()),
    ('end_station', pa.int64()),
    ('end_lat', pa.float64()),
    ('end_lon', pa.float64()),
    ('bike_id', pa.float64()),
    ('plan_duration', pa.int64()),
    ('trip_route_category', pa.string()),
    ('passholder_type', pa.string()),
    ('bike_type', pa.string()),
    ('Y', pa.int64()),
    ('Q', pa.int64()),
    ('M', pa.int64()),        
    ('D', pa.int64())
])

# Pandas dtypes
pandas_dtypes = {
    'trip_id': 'Int64',
    'duration': 'Int64',
    'start_station': 'Int64',
    'start_lat': 'float64',
    'start_lon': 'float64',
    'end_station': 'Int64',
    'end_lat': 'float64',
    'end_lon': 'float64',
    'bike_id': 'int64',
    'plan_duration': 'Int64',
    'trip_route_category': 'object',
    'passholder_type': 'object',
    'bike_type': 'object'
}
