import pandas as pd
# import os

if 'data_exporter' not in globals():
    from mage_ai.data_preparation.decorators import data_exporter

@data_exporter
def export_data_to_big_query(df: pd.DataFrame, **kwargs) -> None:
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

    


    print('success :)')