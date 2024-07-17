import io
import pandas as pd
import os
import requests
import zipfile
from google.cloud import storage
from bs4 import BeautifulSoup

if 'data_loader' not in globals():
    from mage_ai.data_preparation.decorators import data_loader
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test

def scrape_links(url, *args, **kwargs):

    headers = {'user-agent': 'student-project'}

    # Get the HTML content of the page
    response = requests.get(url, headers=headers)
    if response.status_code == 200:
        html_content = response.text
    else:
        print(f"Failed to retrieve the page. Status code: {response.status_code}")
        exit()

    # Parse the HTML content using Beautiful Soup
    soup = BeautifulSoup(html_content, 'html.parser')

    # Find all the links page for trip upload files
    links = soup.find_all('a')
    links = [link for link in links if 'wp-content/uploads' in link.get('href') and 'trips' in link.get('href')]

    # Print the href attribute of each link
    for link in links:
        href = link.get('href')
        print(href)

scrape_links('https://www.rideindego.com/about/data/')



@data_loader
def load_data_from_api(*args, **kwargs):
    return 1
#     #  Reading data from Indego CSV
  
  
#     # headers = {'User-Agent':'student-application'}
    
#     # url = f'https://www.rideindego.com/wp-content/uploads/2024/04/indego-trips-{year}-q{quarter}.zip' 
#     # response = requests.get(url, headers=headers)

#     # # download response content to zipped file
#     # local_zip_path = f'indego-trips-{year}-q{quarter}.zip'

#     # with open(local_zip_path, 'wb') as file:
#     #     file.write(response.content)

#     # # extract zipped file
#     # extracted_folder_path = f'indego-trips-{year}-q{quarter}'

#     # with zipfile.ZipFile(local_zip_path, 'r') as zip_ref:
#     #     zip_ref.extractall(extracted_folder_path)
#     # extracted_folder_path = f'indego-trips-{year}-q{quarter}'

#     # # generate test CSV for schema
#     # csv_filename = f'indego-trips-{year}-q{quarter}.csv'
#     # test_df = pd.read_csv(f'{extracted_folder_path}/{csv_filename}', sep=',', nrows=100)

#     # # Step 3: Upload the CSV file to Google Cloud Storage
#     # bucket_name = 'your-bucket-name'  # Replace with your GCS bucket name
#     # destination_blob_name = os.path.basename(csv_file_name)  # Use the CSV file name as the blob name

#     # # Initialize a GCS client
#     # storage_client = storage.Client(credentials=credentials)
#     # bucket = storage_client.bucket(bucket_name)
#     # blob = bucket.blob(destination_blob_name)

#     # # Upload the file
#     # blob.upload_from_filename(csv_file_name)
#     # return test_df
#     return 1
    # return response.status_code

@test
def test_output(output, *args) -> None:
    assert output is not None, 'error'
    # assert output == 200, f'status code: {output}'
