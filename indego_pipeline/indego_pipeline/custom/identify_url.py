import requests
from bs4 import BeautifulSoup
import pandas as pd
from indego_pipeline.utils.set_date import previous_quarter

if 'custom' not in globals():
    from mage_ai.data_preparation.decorators import custom
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test

@custom
def identify_zipfile(*args, **kwargs):

    year, quarter = previous_quarter(kwargs['execution_date'])

    url = kwargs['indego_url']

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
    
    # find all links for trip upload files
    links = soup.find_all('a')

    # case control for easier parsing
    urls = [link.get('href').lower() for link in links]

    # get url for trip uploads matching the year and quarter
    urls = [url for url in urls
        if 'trips' in url
        and 'wp-content' in url
        and f'{year}' in url
        and f'q{quarter}' in url]
    
    # there should be only 1 url that matches the year and quarter in question
    if len(urls) == 1:
        zipfile_url = urls[0]
        return(zipfile_url)
    elif len(urls) > 1:
        print('error: multiple urls matching year/quarter')
        for url in urls:
            print(url)
        return None
    elif len(urls) == 0:
        print('error: no urls found for target year/quarter')