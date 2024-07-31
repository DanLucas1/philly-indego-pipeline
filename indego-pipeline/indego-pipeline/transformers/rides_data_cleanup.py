import numpy as np
import pandas as pd
if 'transformer' not in globals():
    from mage_ai.data_preparation.decorators import transformer
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test

year = 2024
quarter = 2

@transformer
def transform(data, *args, **kwargs):

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

    # remove rides with invalid latitude or longitudes (but keep nulls)
    data = data.loc[
        (((data['start_lat'] >= -90) & (data['start_lat'] <= 90))) | (data['start_lat'].isna()) &
        (((data['start_lon'] >= -180) & (data['start_lon'] <= 180))) | (data['start_lon'].isna()) &
        (((data['end_lat'] >= -90) & (data['end_lat'] <= 90))) | (data['end_lat'].isna()) &
        (((data['end_lon'] >= -180) & (data['end_lon'] <= 180 | (data['end_lon'].isna()))))
        ]

    # we will accept missing values for the following columns:
        # plan_duration: should be a positive integer or 0 for single-day use
        # passholder_type: '---'
        # bike_type: '---'

    return data

@test
def test_output(output, *args) -> None:

    output_schema = {
        'trip_id': 'Int64',
        'duration': 'int64',
        'start_time': 'datetime64[ns]',
        'end_time': 'datetime64[ns]',
        'start_station': 'Int64',
        'start_lat': 'float64',
        'start_lon': 'float64',
        'end_station': 'Int64',
        'end_lat': 'float64',
        'end_lon': 'float64',
        'bike_id': 'float64',
        'plan_duration': 'Int64',
        'trip_route_category': 'object',
        'passholder_type': 'object',
        'bike_type': 'object'}

    # check that all column dtypes match the required schema
    dtype_mismatch = [
        col for col in output.columns if
        col in output_schema.keys() and output[col].dtype != output_schema[col]
        ]
    dtypes_correct = len(dtype_mismatch) == 0

    # check that all columns are present
    missing_columns = [
        col for col in output_schema.keys()
        if col not in output.columns]
    columns_complete = len(missing_columns) == 0

    assert dtypes_correct, f'columns with incorrect dtypes: {dtype_mismatch}'
    assert columns_complete, f'missing columns: {missing_columns}'
    assert output is not None, 'The output is undefined'
    assert output.trip_id.nunique() == output.shape[0], 'dataset contains duplicate trip IDs'