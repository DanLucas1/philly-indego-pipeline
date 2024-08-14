import os
import pandas as pd
import google.auth
import pandas_gbq
from google.cloud import bigquery

if 'data_exporter' not in globals():
    from mage_ai.data_preparation.decorators import data_exporter

# default credentials will resolve to the attached service account
credentials, project = google.auth.default()
print('authenticated google service account via default credentials')

@data_exporter
def export_data_to_big_query(df: pd.DataFrame, **kwargs) -> None:

    test = os.environ.get('PROJECT_NAME')
    print(type(test))
    print(test[:-1])
    print('P' in test)
    # ---- SETUP ----

    # specify cloud project resources
    project_id = os.environ.get('GCLOUD_PROJECT')
    dataset = os.environ.get('BQ_DATASET')
    table_name = kwargs['bq_table']
    table_id = f'{dataset}.{table_name}'


    # ---- WRITE TO BIGQUERY ----

    # initialize bigquery client
    client = bigquery.Client()

    schema = [
        {'name': 'neighborhood_name', 'type': 'STRING'},
        {'name': 'shape_leng', 'type': 'FLOAT'},
        {'name': 'shape_area', 'type': 'FLOAT'},
        {'name': 'geometry', 'type': 'STRING'}
    ]

    # generate table object
    table = bigquery.Table(
        f'{project_id}.{dataset}.{table_name}',
         schema=schema)

    # create the table
    client.create_table(table, exists_ok=True)

    try:
        pandas_gbq.to_gbq(
            df,
            table_id,
            table_schema = schema,
            project_id=project_id,
            if_exists='replace')
        print(f'wrote {df.shape[0]} records to dataset {project_id}.{table_id}')
    except Exception as E:
        print(f'could not write data to BigQuery: {E}') 