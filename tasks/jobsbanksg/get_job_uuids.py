# Importing necessary libraries
import os
import numpy as np
from tqdm import tqdm
import logging
import csv
import sys

import requests
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry


def requests_retry_session(
    retries=3,
    backoff_factor=0.3,
    status_forcelist=(500, 502, 504),
    session=None,
):
    session = session or requests.Session()
    retry = Retry(
        total=retries,
        read=retries,
        connect=retries,
        backoff_factor=backoff_factor,
        status_forcelist=status_forcelist,
    )
    adapter = HTTPAdapter(max_retries=retry)
    session.mount('http://', adapter)
    session.mount('https://', adapter)
    return session


if __name__=='__main__':

    if len(sys.argv) == 1:
        base_path = os.getcwd()
    else:
        base_path = sys.argv[1]

    logging.basicConfig(format='%(asctime)s - %(message)s', level=logging.INFO)

    # Creating request session to allow for retrying
    requestsSession = requests.Session()

    # Get total number of current job postings
    initial_search_response = requests.get(
        'https://api.mycareersfuture.sg/v2/jobs?limit=100&page=0&sortBy=new_posting_date',
        timeout=5)

    # Determine number of iterations required to get all job postings as each
    # iteration only returns 100 job postings
    number_of_listings = initial_search_response.json()['countWithoutFilters']
    search_iterations = np.where(
        number_of_listings % 100 > 0,
        int(number_of_listings / 100) + 1,
        int(number_of_listings / 100))

    # Extracting job posting uuids
    job_uuids = []
    for i in tqdm(range(0, search_iterations)):
        search_response = requests_retry_session(session=requestsSession).get(
            'https://api.mycareersfuture.sg/v2/jobs?limit=100&page={}&sortBy=new_posting_date'.format(i),
            timeout=3.0)
        job_uuids += [response['uuid'] for response in search_response.json()['results']]

    with open(os.path.join(base_path, 'data/job_uuids.csv'), 'w') as f:
        wr = csv.writer(f, delimiter='\n')
        wr.writerow(job_uuids)

    logging.info('Saved job uuids!')
