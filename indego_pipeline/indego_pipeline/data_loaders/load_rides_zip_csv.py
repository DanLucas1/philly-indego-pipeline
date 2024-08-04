import io
import requests
import zipfile
import pandas as pd
from indego_pipeline.utils.set_date import previous_quarter
from indego_pipeline.utils.schemas import dtypes_read

if 'data_loader' not in globals():
    from mage_ai.data_preparation.decorators import data_loader
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test

@data_loader
def download_zip(zipfile_url, *args, **kwargs):

    # set the target year/quarter to previous quarter
    year, quarter = previous_quarter(kwargs['execution_date'])

    headers = {'user-agent': 'student-project', 'Accept-Encoding': 'identity'}
    response = requests.get(zipfile_url, headers=headers)

    local_zip_path = f'rides-{year}-q{quarter}.zip'

    # define column dtypes for csv read
    date_cols = ['start_time', 'end_time']

    if response.status_code == 200:
        # writing file to the local disk
        with open(local_zip_path, 'wb') as file:
            file.write(response.content)
        print(f'Saved zipfile "rides-{year}-q{quarter}.zip" to local disk')

        # navigate to the trips csv within the zip archive and write to google cloud storage
        with zipfile.ZipFile(local_zip_path, 'r') as zip_ref:
            info = zip_ref.infolist() # list all contents
            for file_info in info: # isolate to only trips csv
                if file_info.filename.endswith('.csv') and '/' not in file_info.filename and 'stations' not in file_info.filename:
                    with zip_ref.open(file_info) as csv_file:
                        df = pd.read_csv(csv_file, dtype=dtypes_read, parse_dates=date_cols)
                    print(f'read {df.shape[0]} rows from csv to datafame')
        return df
    else:
        print(f"Failed to retrieve the page. Status code: {response.status_code}")
        return None

@test
def test_output(output, *args) -> None:
    assert output is not None