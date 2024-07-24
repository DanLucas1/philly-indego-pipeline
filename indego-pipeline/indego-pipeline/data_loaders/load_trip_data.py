import io
import pandas as pd
import os
import requests
import zipfile
from google.cloud import storage
from bs4 import BeautifulSoup

if 'custom' not in globals():
    from mage_ai.data_preparation.decorators import custom
if 'data_loader' not in globals():
    from mage_ai.data_preparation.decorators import data_loader
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test

year = 2024
quarter = 2

@custom
def identify_zipfile(url):

    # retrieve HTML content
    headers = {'user-agent': 'student-project'}
    response = requests.get(url, headers=headers)

    if response.status_code == 200:
        html_content = response.text
    else:
        print(f"Failed to retrieve the page. Status code: {response.status_code}")
        exit()

    # parse HTML content
    soup = BeautifulSoup(html_content, 'html.parser')
    
    # find all links for trip upload files and
    links = soup.find_all('a')
    urls = [link.get('href').lower() for link in links]

    # get url for trip uploads matching the year and quarter
    urls = [url for url in urls
        if 'trips' in url
        and 'wp-content' in url
        and f'{year}' in url
        and f'q{quarter}' in url]
    
    if len(urls) == 1:
        return urls[0]
    else:
        print('error: could not identify single url')
        for url in urls:
            print(url)
        return None

zipfile_url = identify_zipfile('https://www.rideindego.com/about/data/')

@data_loader
def download_zip(*args, **kwargs):
    
    headers = {'user-agent': 'student-project', 'Accept-Encoding': 'identity'}
    response = requests.get(zipfile_url, stream=True, headers=headers)

    save_path = f'rides-{year}-q{quarter}.zip'

    if response.status_code == 200:

        # writing file to the local disk
        with open(save_path, 'wb') as file:
            file.write(response.content)
        print(f'Saved zipfile "rides-{year}-q{quarter}.zip" to local disk')
        return 1
    else:
        print(f"Failed to retrieve the page. Status code: {response.status_code}")
        return 0

@test
def test_output(output, *args) -> None:
    assert output is not None, 'error'
    # assert output == 200, f'status code: {output}'
