import io
import requests
import os
import geopandas as gpd
import pandas as pd

if 'data_loader' not in globals():
    from mage_ai.data_preparation.decorators import data_loader
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test

@data_loader
def load_philadelphia_neighborhoods(*args, **kwargs):

    # # read geojson information from neighborhoods file
    url = kwargs['neighborhood_file_url']
    gdf = gpd.read_file(f'geojson:{url}')
    gdf.geometry = gdf.geometry.to_crs('EPSG:4326')

    # convert to pandas df to pass between blocks
    df = pd.DataFrame(gdf)
    df['geometry'] = df['geometry'].apply(lambda geom: geom.wkt)

    return df

@test
def test_output(output, *args) -> None:
    assert output is not None, 'The output is undefined'
