import numpy as np
import pandas as pd
if 'transformer' not in globals():
    from mage_ai.data_preparation.decorators import transformer
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test


@transformer
def transform(data, *args, **kwargs):

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
    data = data.drop_duplicates(subset='trip_id')

    # remove rides with invalid latitude or longitudes (but keep nulls)
    data = data.loc[
        (((data['start_lat'] >= -90) & (data['start_lat'] <= 90))) | (data['start_lat'].isna()) &
        (((data['start_lon'] >= -180) & (data['start_lon'] <= 180))) | (data['start_lon'].isna()) &
        (((data['end_lat'] >= -90) & (data['end_lat'] <= 90))) | (data['end_lat'].isna()) &
        (((data['end_lon'] >= -180) & (data['end_lon'] <= 180 | (data['end_lon'].isna()))))
        ]

    # missing values are acceptable for the following columns:
    # plan_duration: should be a positive integer or 0 for single-day use
    # passholder_type: '---'
    # bike_type: '---'

    return data

@test
def test_output(output, *args) -> None:

    output.trip_id.nunique() == output.shape[0], 'duplicate trip IDs in dataset'

    assert output is not None, 'The output is undefined'
