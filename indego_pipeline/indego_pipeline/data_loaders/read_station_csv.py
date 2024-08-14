import io
import pandas as pd
import requests
if 'data_loader' not in globals():
    from mage_ai.data_preparation.decorators import data_loader
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test


@data_loader
def load_data_from_api(url, *args, **kwargs):

    headers = {'User-Agent': 'student-project'}
    parse_dates = ['Day of Go_live_date']

    return pd.read_csv(url, sep=',', storage_options=headers, parse_dates=parse_dates)


@test
def test_output(output, *args) -> None:
    assert output is not None, 'The output is undefined'
