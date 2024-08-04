import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from indego_pipeline.utils.set_date import previous_quarter
import google.auth
from google.cloud import storage
if 'data_exporter' not in globals():
    from mage_ai.data_preparation.decorators import data_exporter

# default credentials will resolve to the attached service account
credentials, project = google.auth.default()
print('authenticated google service account via default credentials')

@data_exporter
def export_data_to_google_cloud_storage(df: pd.DataFrame, **kwargs) -> None:    

    # set the target year/quarter to previous quarter
    year, quarter = previous_quarter(kwargs['execution_date'])

    # assign Y/M/D columns for cleaner partitioning
    df = df.assign(
        Y = df['start_time'].dt.year,
        Q = df['start_time'].dt.quarter,
        M = df['start_time'].dt.month,
        D = df['start_time'].dt.day)

    # df = df.drop_duplicates(subset=['Y','Q','M','D'])

    # specify schema for pyarrow table
    output_schema = pa.schema([
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

    try:
        table = pa.Table.from_pandas(df, schema=output_schema, preserve_index=False)
        r = table.num_rows
        c = table.num_columns
        print(f'created pyarrow table for Q{quarter}-{year}: {r} rows, {c} columns')
    except exception as E:
        print(f'could not create table: {E}')

    # generate GCS object
    rides_filesystem = pa.fs.GcsFileSystem()

    # set parameters for path
    bucket = kwargs['bucket_name']

    # write dataset to specified path
    pq.write_to_dataset(
        table,
        root_path=bucket,
        partition_cols=['Y', 'Q', 'M', 'D'],
        basename_template = 'trips{i}.parquet',
        filesystem = rides_filesystem
    )