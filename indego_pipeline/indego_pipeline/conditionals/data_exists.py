from indego_pipeline.utils.set_date import previous_quarter
import google.auth
from google.cloud import storage
import sys
if 'condition' not in globals():
    from mage_ai.data_preparation.decorators import condition

# default credentials will resolve to the attached service account
credentials, project = google.auth.default()
print('authenticated google service account via default credentials')

@condition
def evaluate_condition(*args, **kwargs) -> bool:

    # set the target year/quarter to previous quarter
    year, quarter = previous_quarter(kwargs['execution_date'])

    # generate client
    storage_client = storage.Client()

    # construct blob path to check
    bucket_name = kwargs['bucket_name']
    blob_path = f'Y={year}/Q={quarter}/'
    
    # build bucket and blob objects
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(blob_path)
    if blob.exists(storage_client):
        print(f'data for Q{quarter} {year} already written to {bucket_name}/{blob_path}')
        return False
    else:
        return True
        print(f'Data for Q{quarter} {year} not found in cloud storage, continuing pipeline')
