import pandas as pd
import google.auth
from pandas import DataFrame
from os import path
import pyarrow as pa
import pyarrow.parquet as pq
import pyarrow.dataset as ds
import os
from datetime import datetime
from dateutil.relativedelta import relativedelta


from os import path
if 'data_loader' not in globals():
    from mage_ai.data_preparation.decorators import data_loader
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test

# default credentials will resolve to the attached service account
credentials, project = google.auth.default()
print('authenticated google service account via default credentials')


@data_loader
def load_from_gcs(*args, **kwargs):

    # set the target year/quarter to previous quarter
    now = kwargs.get('execution_date')
    target = now - relativedelta(months=3)
    year = target.year
    quarter = pd.Timestamp(target).quarter

    # bucket name matches bucket defined in variables.tf
    bucket_name = 'indego_815299289556'
    gcs_path = f'{bucket_name}/Y={year}/Q={quarter}/'

    # generate GCS object
    gcs = pa.fs.GcsFileSystem()

    # # specify partitioning based on taxi type
    partitioning = ds.partitioning(
        schema=pa.schema([
            ('Y', pa.int32()),
            ('Q', pa.string()),
            ('M', pa.int32()),
            ('D', pa.int32())
            ]),
        flavor='hive'
    )

    # read the parquet files at the gcs path and combine
    pq_from_gcs = ds.dataset(
        gcs_path,
        filesystem=gcs,
        format='parquet',
        partitioning=partitioning)

    dtypes = {
        'trip_id': 'Int64',
        'duration': 'Int64',
        'start_station': 'Int64',
        'start_lat': 'float64',
        'start_lon': 'float64',
        'end_station': 'Int64',
        'end_lat': 'float64',
        'end_lon': 'float64',
        'bike_id': 'object',
        'plan_duration': 'Int64',
        'trip_route_category': 'object',
        'passholder_type': 'object',
        'bike_type': 'object'}

    # cast dataset to pandas df
    df = pq_from_gcs.to_table().to_pandas().astype(dtypes)

    # drop the partition columns
    df = df.drop(columns=['Y', 'Q', 'M', 'D'])
    
    return df

@test
def test_output(output, *args) -> None:
    assert output is not None, 'The output is undefined'