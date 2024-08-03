import pandas as pd
from pandas import DataFrame
import os
from os import path
import pandas_gbq
from google.cloud import bigquery

if 'data_exporter' not in globals():
    from mage_ai.data_preparation.decorators import data_exporter

# default credentials will resolve to the attached service account
credentials, project = google.auth.default()
print('authenticated google service account via default credentials')


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
    bigquery.SchemaField('bike_type', 'STRING')]

    
@data_exporter
def export_data_to_big_query(df: DataFrame, **kwargs) -> None:

    ## ---- SETUP ----

    # load parameters from kwargs
    # project_id = kwargs.get('project_id')
    # dataset = kwargs.get('bq_dataset_name')

    # specify cloud project resources
    project_id = 'indego-pipeline'
    dataset = 'indego_tripdata'
    table_name = 'fact_indego_rides'

    ## ---- WRITE TO BIGQUERY ----

    # initialize bigquery client
    client = bigquery.Client()

    # generate table object
    table = bigquery.Table(
        f'{project_id}.{dataset}.{table_name}',
         schema=bq_schema)

    # specify partitioning and clustering
    table.time_partitioning = bigquery.TimePartitioning(  # define partitioning field
        type_=bigquery.TimePartitioningType.DAY,
        field=start_time)

    table.clustering_fields = ['bike_id']  # define clustering field

    # create the table
    client.create_table(table, exists_ok=True)

    try:
        pandas_gbq.to_gbq(
            df,
            table_id,
            project_id=project_id,
            # chunksize=10000,
            if_exists='append')
        print(f'wrote {df.shape[0]} records to dataset {project_id}.{table_id}')
    except Exception as E:
        print(f'could not write data to BigQuery: {E}')