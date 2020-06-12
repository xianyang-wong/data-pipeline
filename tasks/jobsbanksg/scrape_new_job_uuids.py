# Importing necessary libraries
import os
import pandas as pd
import json
import time
import logging
import sys

import requests

if len(sys.argv) == 1:
    base_path = os.getcwd()
else:
    base_path = sys.argv[1]
sys.path.append(base_path)
from utils import config, db_connection_helper as db, requests_util

if __name__ == '__main__':

    logging.basicConfig(format='%(asctime)s - %(message)s', level=logging.INFO)

    # Connection to database
    db_cfg = config.get_config()['aws_db']
    host = db_cfg['host']
    port = db_cfg['port']
    user = db_cfg['user']
    password = db_cfg['password']
    database = db_cfg['database']

    db_conn = db.rdbmsConnectionHelper(
        user=user,
        password=password,
        host=host,
        database=database,
        port=port)

    # Creating request session to allow for retrying
    requestsSession = requests.Session()

    # Read in new job uuids from saved .csv
    job_uuids = pd.read_csv(os.path.join(base_path, 'data/job_uuids.csv'), header=None).iloc[:, 0].to_list()

    extracted_jobs = db_conn.get_raw_data('''
          SELECT job_uuid
          FROM jobsbanksg.tbl_jobs_api;
      ''')

    extracted_job_uuids = [job[0] for job in extracted_jobs]

    new_job_uuids = list(set(job_uuids) - set(extracted_job_uuids))

    # Scrape data
    total_job_uuids = len(new_job_uuids)
    counter = 0
    while len(new_job_uuids) != 0:

        job_response = requests_util.requests_retry_session(session=requestsSession).get(
            'https://api.mycareersfuture.sg/v2/jobs/{}'.format(new_job_uuids[0]),
            timeout=5)

        job_uuid = job_response.json()['uuid']
        source_code = job_response.json()['sourceCode']
        job_title = job_response.json()['title']
        job_posting_date = job_response.json()['metadata']['originalPostingDate']
        job_closing_date = job_response.json()['metadata']['expiryDate']
        experience_min = job_response.json()['minimumYearsExperience']
        if not job_response.json()['metadata']['isHideSalary']:
            salary_min = job_response.json()['salary']['minimum']
            salary_max = job_response.json()['salary']['maximum']
            salary_type = job_response.json()['salary']['type']['salaryType']
        else:
            salary_min = None
            salary_max = None
            salary_type = None
        job_description = job_response.json()['description']
        try:
            company_name = job_response.json()['postedCompany']['name']
            company_uen = job_response.json()['postedCompany']['uen']
        except Exception as e:
            company_name = None
            company_uen = None
        if not job_response.json()['metadata']['isHideCompanyAddress']:
            address_block = job_response.json()['address']['block']
            address_street = job_response.json()['address']['street']
            address_floor = job_response.json()['address']['floor']
            address_unit = job_response.json()['address']['unit']
            address_building = job_response.json()['address']['building']
            address_postal_code = job_response.json()['address']['postalCode']
        else:
            address_block = None
            address_street = None
            address_floor = None
            address_unit = None
            address_building = None
            address_postal_code = None
        blob = json.dumps(job_response.json())

        data_record = pd.DataFrame([[job_uuid, source_code, job_title,
                                     company_name,
                                     job_posting_date, job_closing_date, experience_min,
                                     salary_min, salary_max, salary_type,
                                     job_description,
                                     address_block, address_street, address_floor, address_unit,
                                     address_building, address_postal_code,
                                     blob]])

        data_record.columns = ['job_uuid', 'source_code', 'job_title',
                               'company_name',
                               'job_posting_date', 'job_closing_date', 'experience_min',
                               'salary_min', 'salary_max', 'salary_type',
                               'job_description',
                               'address_block', 'address_street', 'address_floor', 'address_unit',
                               'address_building', 'address_postal_code',
                               'blob']

        # Save to database
        db_conn.execute_batch_insert('jobsbanksg.tbl_jobs_api', data_record)
        time.sleep(1)

        new_job_uuids.pop(0)
        counter += 1
        logging.info('Scraped {} out of {} jobs!'.format(counter, total_job_uuids))
