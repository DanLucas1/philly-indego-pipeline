import numpy as np
import pandas as pd
from indego_pipeline.utils.set_date import previous_quarter
from indego_pipeline.utils.ride_schemas import dtypes_write
if 'transformer' not in globals():
    from mage_ai.data_preparation.decorators import transformer
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test

@transformer
def transform(data, *args, **kwargs):

    # set the target year/quarter to previous quarter
    year, quarter = previous_quarter(kwargs['execution_date'])

    # start times should fall within the year and quarter (end times are coerced to 24 hours max duration)
    data = data.loc[
        (data.start_time.dt.year == year) &
        (data.start_time.dt.quarter == quarter)
        ]

    # remove null and invalid bike IDs (e.g. 'WAND_RUSSELL')
    data['bike_id'] = pd.to_numeric(data['bike_id'], errors='coerce', downcast='integer')
    data = data.dropna(subset='bike_id')

    # trip duration must be more than 0 minutes
    data = data.loc[data.duration > 0]
    data = data.assign(
        duration = np.where(
            data.duration > 0,
            data.duration,
            ((data['end_time'] - data['start_time']).dt.total_seconds() / 60).round().astype(int))
    )

    # fix any falsely labelled 'Round Trip' rides
    data.loc[
        (data.trip_route_category == 'Round Trip') &
        (data.start_station != data.end_station),
        'trip_route_category'] = 'One Way'

    # fix any falsely labelled 'One Way' rides
    data.loc[
        (data.trip_route_category == 'One Way') &
        (data.start_station == data.end_station),
        'trip_route_category'] = 'Round Trip'

    # remove duplicate trip_ids
    data = data.drop_duplicates(subset='trip_id', ignore_index=True)

    # correct improperly signed lat/lon and keep valid coordinates and nulls
    lat_cols = [col for col in data.columns if col.endswith('_lat')]
    for col in lat_cols:
        data[col] = data[col].abs()
        data = data.loc[((data[col] >= 39.5) & (data[col] <= 40.5)) | (data[col].isna())]

    lon_cols = [col for col in data.columns if col.endswith('_lon')]
    for col in lon_cols:
        data[col] = -1 * data[col].abs()  # Ensure longitude is negative
        data = data.loc[((data[col] >= -75.5) & (data[col] <= -74.5)) | (data[col].isna())]

    # column bike_type was not present until Q3 2018 data
    if year < 2018 or (year <= 2018 and quarter <= 2) and 'bike_type' not in data.columns:
        data = data.assign(bike_type = '')

    # ensure proper dtypes
    data = data.astype(dtypes_write)

    return data

@test
def test_output(output, *args) -> None:

    # check that all column dtypes match the required schema
    dtype_mismatch = [
        col for col in output.columns
        if col in dtypes_write.keys()
        and output[col].dtype != dtypes_write[col]
    ]

    dtypes_correct = len(dtype_mismatch) == 0

    # check that all columns are present
    missing_columns = [
        col for col in dtypes_write.keys()
        if col not in output.columns]
    columns_complete = len(missing_columns) == 0

    assert dtypes_correct, f'columns with incorrect dtypes: {dtype_mismatch}'
    assert columns_complete, f'missing columns: {missing_columns}'
    assert output is not None, 'The output is undefined'
    assert output.trip_id.nunique() == output.shape[0], 'dataset contains duplicate trip IDs'