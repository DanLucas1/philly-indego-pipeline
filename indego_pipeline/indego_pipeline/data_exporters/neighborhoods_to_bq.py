import os
import pandas as pd
import google.auth
import pandas_gbq
from google.cloud import bigquery

import geopandas as gpd
from shapely import wkt
# from shapely.geometry import Point


if 'data_exporter' not in globals():
    from mage_ai.data_preparation.decorators import data_exporter

# default credentials will resolve to the attached service account
credentials, project = google.auth.default()
print('authenticated google service account via default credentials')

@data_exporter
def export_data_to_big_query(df: pd.DataFrame, **kwargs) -> None:

    ## ---- SETUP ----

    # specify cloud project resources
    # project_id = kwargs['project_id']
    # dataset = kwargs['bq_dataset']
    # table_name = kwargs['bq_table']
    # table_id = f'{dataset}.{table_name}'

    project_id = 'indego-pipeline'
    dataset = 'indego_tripdata'
    table_name = 'philadelphia_neighborhoods'
    table_id = f'{dataset}.{table_name}'


    ## ---- WRITE TO BIGQUERY ----

    # initialize bigquery client
    client = bigquery.Client()

    # BigQuery schema
    bq_schema = [
        bigquery.SchemaField('neighborhood_name', 'STRING'),
        bigquery.SchemaField('shape_leng', 'FLOAT'),
        bigquery.SchemaField('shape_area', 'FLOAT'),
        bigquery.SchemaField('geometry', 'GEOGRAPHY')]

    # generate table object
    table = bigquery.Table(
        f'{project_id}.{dataset}.{table_name}',
         schema=bq_schema)

    # create the table
    client.create_table(table, exists_ok=True)

    try:
        pandas_gbq.to_gbq(
            df,
            table_id,
            project_id=project_id,
            if_exists='replace')
        print(f'wrote {df.shape[0]} records to dataset {project_id}.{table_id}')
    except Exception as E:
        print(f'could not write data to BigQuery: {E}')









# if 'custom' not in globals():
#     from mage_ai.data_preparation.decorators import custom
# if 'test' not in globals():
#     from mage_ai.data_preparation.decorators import test

# def label_neighborhoods(rides_df, geo_df, point_type) -> pd.DataFrame:

#     # specify columns for output (input columns + neighborhood label)
#     keep_cols = list(rides_df.columns) + [f'{point_type}_neighborhood']

#     # columns for constructing start/end geometry
#     lat_col = f'{point_type}_lat'
#     lon_col = f'{point_type}_lon'
#     geometry_col = f'{point_type}_geometry'


#     # set CRS and join
#     gdf = gdf.set_crs('EPSG:4326') 
#     labeled_df = gdf.sjoin(geo_df, how='left', predicate='intersects')

#     # rename LISTNAME column to appropriate label and keep columns defined earlier
#     labeled_df = labeled_df.rename(columns={'LISTNAME': f'{point_type}_neighborhood'})
#     labeled_df = labeled_df[keep_cols]
        
#     return labeled_df

# @custom
# def transform_custom(rides, neighborhoods, *args, **kwargs):

#     # convert neighborhoods df back to gdf
#     neighborhoods['geometry'] = neighborhoods['geometry'].apply(wkt.loads)
#     neighborhoods = gpd.GeoDataFrame(neighborhoods, geometry='geometry')

#     # apply labeling function to start and end lat/long columns
#     rides = label_neighborhoods(rides, neighborhoods, 'start')
#     rides = label_neighborhoods(rides, neighborhoods, 'end')

#     return rides.astype(dtypes_labeled)

# @test
# def test_output(output, *args) -> None:
#     assert type(output) == pd.DataFrame, 'invalid output type'
#     assert 'start_neighborhood' in output.columns, 'missing start_neighborhood column'
#     assert 'end_neighborhood' in output.columns, 'missing end_neighborhood column'