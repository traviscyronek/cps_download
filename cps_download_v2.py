"""
File:    cps_download_v2.py
Author:  Travis Cyronek
Purpose: Download CPS basic monthly files from the NBER website
"""


# imports
from bs4 import BeautifulSoup
import os
import requests


# user inputs
save_loc = '/users/traviscyronek/dropbox/data/bls/cps/csv/'


def downloader(file_type, save_loc):

    """
    Description: downloads cps monthly files if not already downloaded in save_loc

    Inputs:
        file_type -- (str) either 'csv', 'dta', or 'raw'
        save_loc  -- (str) directory to write to
    """

    url = 'http://data.nber.org/data/cps-basic2/' + file_type + '/'

    soup = BeautifulSoup(requests.get(url).content)

    links = [f"{url}{item['href']}" for item in soup.select("a[href$='."+file_type+"']")]

    for link in links:
        file_name = link.split('/')[-1]
        if os.path.exists(save_loc+file_name):
            print(f'{link} already downloaded. Skipping...')
        else:
            print(f'Downloading {link}')
            results = requests.get(link)
            with open(save_loc+file_name, 'wb') as f:
                f.write(results.content)

    print('Download Complete!')


if __name__ == '__main__':
    downloader(file_type, save_loc)
