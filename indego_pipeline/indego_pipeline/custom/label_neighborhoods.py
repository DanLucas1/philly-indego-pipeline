import geopandas as gpd
import pandas as pd
from shapely.geometry import Point
from shapely import wkt
from indego_pipeline.utils.schemas import dtypes_labeled

if 'custom' not in globals():
    from mage_ai.data_preparation.decorators import custom
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test

def label_neighborhoods(rides_df, geo_df, point_type) -> pd.DataFrame:

    # specify columns for output (input columns + neighborhood label)
    keep_cols = list(rides_df.columns) + [f'{point_type}_neighborhood']

    # columns for constructing start/end geometry
    lat_col = f'{point_type}_lat'
    lon_col = f'{point_type}_lon'
    geometry_col = f'{point_type}_geometry'

    # generate geometry column and convert to gpd
    rides_df[geometry_col] = [Point(xy) for xy in zip(rides_df[lon_col], rides_df[lat_col])]    
    gdf = gpd.GeoDataFrame(rides_df, geometry=geometry_col)

    # set CRS and join
    gdf = gdf.set_crs('EPSG:4326') 
    labeled_df = gdf.sjoin(geo_df, how='left', predicate='intersects')

    # rename LISTNAME column to appropriate label and keep columns defined earlier
    labeled_df = labeled_df.rename(columns={'LISTNAME': f'{point_type}_neighborhood'})
    labeled_df = labeled_df[keep_cols]
        
    return labeled_df

@custom
def transform_custom(rides, neighborhoods, *args, **kwargs):

    # convert neighborhoods df back to gdf
    neighborhoods['geometry'] = neighborhoods['geometry'].apply(wkt.loads)
    neighborhoods = gpd.GeoDataFrame(neighborhoods, geometry='geometry')

    # apply labeling function to start and end lat/long columns
    rides = label_neighborhoods(rides, neighborhoods, 'start')
    rides = label_neighborhoods(rides, neighborhoods, 'end')

    return rides.astype(dtypes_labeled)

@test
def test_output(output, *args) -> None:
    assert type(output) == pd.DataFrame, 'invalid output type'
    assert 'start_neighborhood' in output.columns, 'missing start_neighborhood column'
    assert 'end_neighborhood' in output.columns, 'missing end_neighborhood column'