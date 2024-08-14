import os
import pandas as pd
import google.auth
import pandas_gbq
from google.cloud import bigquery
from indego_pipeline.utils.ride_schemas import bq_schema

if 'data_exporter' not in globals():
    from mage_ai.data_preparation.decorators import data_exporter

# default credentials will resolve to the attached service account
credentials, project = google.auth.default()
print('authenticated google service account via default credentials')

@data_exporter
def export_data_to_big_query(df: pd.DataFrame, **kwargs) -> None:

    ## ---- SETUP ----

    # specify cloud project resources
    project_id = os.environ.get('GCLOUD_PROJECT')
    dataset = os.environ.get('BQ_DATASET')
    table_name = kwargs['bq_table']
    table_id = f'{dataset}.{table_name}'

    # dataset partitioning and clustering
    partition_field = 'start_time'
    cluster_fields = ['start_station', 'end_station']


    ## ---- WRITE TO BIGQUERY ----

    # initialize bigquery client
    client = bigquery.Client()

    # generate table object
    table = bigquery.Table(
        f'{project_id}.{dataset}.{table_name}',
         schema=bq_schema)

    # specify partitioning
    table.time_partitioning = bigquery.TimePartitioning(
        type_=bigquery.TimePartitioningType.DAY,
        field=partition_field)

    # define clustering field
    table.clustering_fields = cluster_fields

    # create the table
    client.create_table(table, exists_ok=True)

    try:
        pandas_gbq.to_gbq(
            df,
            table_id,
            project_id=project_id,
            if_exists='append')
        print(f'wrote {df.shape[0]} records to dataset {project_id}.{table_id}')
    except Exception as E:
        print(f'could not write data to BigQuery: {E}')