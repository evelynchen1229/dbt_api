import requests
import pandas as pd
import numpy as np
import os

API_KEY = os.getenv('DBT_API_KEY')
# moonpig Minoro project: account_ID 801, project_id = 1031
ACCOUNT_ID = 801

def dbt_jobs(account_id = ACCOUNT_ID, api_key = API_KEY):
    jobs = []
    job_names = []
    try:
        response = requests.get(
            url = f"https://cloud.getdbt.com/api/v2/accounts/{account_id}/jobs/"
            ,headers={'Authorization': f"Token {api_key}", 'Content-Type': 'application/json'},
            params ={'project_id':1031,'environment_id':1031}
                )
        if response.status_code == 200:
            job_list = response.json().get('data')
            for job in job_list:
                jobs.append(job['id'])
                job_names.append(job['name'])

            job_set = {'job_id': jobs,
                    'job_name': job_names
                    }

            return job_set

        else:
            return 'Internal Server Error or Page not Found'
    except requests.Timeout as timeout_exception:
        return 'Time Out'
    except requests.HTTPError as http_error:
        return 'Server HTTP Error'
    except requests.ConnectionError as connection_error:
        return 'Connection Error'
