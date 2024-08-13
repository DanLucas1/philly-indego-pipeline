import pandas as pd
import google.auth
from google.cloud import storage
import sys

if 'data_exporter' not in globals():
    from mage_ai.data_preparation.decorators import data_exporter

@data_exporter
def gcs_export_single_file(df, *args, **kwargs):

    csv_data = df.to_csv(index=False)

    # default credentials will resolve to the attached service account
    credentials, project = google.auth.default()
    print('authenticated google service account via default credentials')

    # generate client
    storage_client = storage.Client()

    # construct blob path to check
    bucket_name = kwargs['bucket_name']
    filename = 'stations_list.csv'
    blob_path = f'station_data/{filename}'
    
    # build bucket and blob objects
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(blob_path)

    try:
        blob.upload_from_string(csv_data, content_type='text/csv')
        print(f'uploaded current Indego station list to {bucket_name}/{blob_path}')
    except Exception as E:
        print(f'error uploading: {E}')