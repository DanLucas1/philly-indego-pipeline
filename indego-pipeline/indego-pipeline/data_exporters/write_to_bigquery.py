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
    bigquery.SchemaField('start_time', 'TIMESTAMP')
    bigquery.SchemaField('end_time', 'TIMESTAMP')
    bigquery.SchemaField('start_station', 'INTEGER')
    bigquery.SchemaField('start_lat', 'FLOAT')
    bigquery.SchemaField('start_lon', 'FLOAT')
    bigquery.SchemaField('end_station', 'INTEGER')
    bigquery.SchemaField('end_lat', 'FLOAT')
    bigquery.SchemaField('end_lon', 'FLOAT')
    bigquery.SchemaField('bike_id', 'FLOAT')
    bigquery.SchemaField('plan_duration', 'INTEGER')
    bigquery.SchemaField('trip_route_category', 'STRING')
    bigquery.SchemaField('passholder_type', 'STRING')
    bigquery.SchemaField('bike_type', 'STRING')]

    

@data_exporter
def export_data_to_big_query(df: DataFrame, **kwargs) -> None:

    ## ---- SETUP ----

    # load parameters from kwargs
    project_id = kwargs.get('project_id')
    dataset = kwargs.get('bq_dataset_name')
    taxi_type = kwargs.get('taxi_type')

    # identify partitioning column based on taxi_type
    partition_options = {
        'yellow': 'tpep_pickup_datetime',
        'green': 'lpep_pickup_datetime',
        'fhv': 'pickup_datetime'}
    trip_datetime = partition_options.get(taxi_type)

    # specify bigquery table ID
    table_id = f'{dataset}.{taxi_type}_trips'

    ## ---- WRITE TO BIGQUERY ----

    # initialize bigquery client
    client = bigquery.Client()

    # define partitioning and clustering
    table = bigquery.Table(f'{project_id}.{table_id}', schema=schemas.get(taxi_type, None))
    table.time_partitioning = bigquery.TimePartitioning(  # define partitioning field
        type_=bigquery.TimePartitioningType.DAY,
        field=trip_datetime
    )
    table.clustering_fields = ['pu_location_id']  # define clustering field

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