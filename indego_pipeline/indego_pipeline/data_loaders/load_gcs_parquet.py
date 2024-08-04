import pandas as pd
import google.auth
import pyarrow as pa
import pyarrow.parquet as pq
import pyarrow.dataset as ds
import os
from indego_pipeline.utils.set_date import previous_quarter
from indego_pipeline.utils.schemas import dtypes_write

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
    year, quarter = previous_quarter(kwargs['execution_date'])

    # bucket name matches bucket defined in variables.tf
    bucket_name = kwargs['bucket_name']
    gcs_path = f'{bucket_name}/Y={year}/Q={quarter}/'

    # generate GCS object
    gcs = pa.fs.GcsFileSystem()

    # specify partitioning to match file structure
    partitioning = ds.partitioning(
        schema=pa.schema([
            ('Y', pa.int32()),
            ('Q', pa.int32()),
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

    # cast dataset to pandas df
    df = pq_from_gcs.to_table().to_pandas().astype(dtypes_write)

    # drop the partition columns
    df = df.drop(columns=['Y', 'Q', 'M', 'D'])
    
    return df

@test
def test_output(output, *args) -> None:
    assert output is not None, 'The output is undefined'