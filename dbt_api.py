import requests
import pandas as pd
import numpy as np
import os

API_KEY = os.getenv('DBT_API_KEY')
# moonpig Minro project: account_ID 801, project_id = 1031
ACCOUNT_ID = 801

def dbt_jobs(account_id = ACCOUNT_ID, api_key = API_KEY):
    response = requests.get(
        url = f"https://cloud.getdbt.com/api/v2/accounts/{account_id}/jobs/"
        ,headers={'Authorization': f"Token {api_key}", 'Content-Type': 'application/json'},
        params ={'project_id':1031}
            )


# get a list of all jobs in the project
jobs = []
job_names = []
res2 = requests.get(
        url = f"https://cloud.getdbt.com/api/v2/accounts/{ACCOUNT_ID}/jobs/"
        ,headers={'Authorization': f"Token {API_KEY}", 'Content-Type': 'application/json'},
        params ={'project_id':1031}
        )
dic2 = res2.json()
job_list = dic2.get('data')
for job in job_list:
    jobs.append(job['id'])
    job_names.append(job['name'])


#test
#expected_jobs = [1960,2325,2520,2521,2744,2933,2974]

# get a list of jobs and succesful runs
final_job = []
final_job_name = []
final_run = []
final_finished_at = []
final_run_duration = []
for job in jobs:
    print(job)
    runs = []
    finished_at = []
    run_duration = []
    res3 = requests.get(
            url = f"https://cloud.getdbt.com/api/v2/accounts/{ACCOUNT_ID}/runs/"
            ,headers={'Authorization': f"Bearer {API_KEY}", 'Accept': 'application/json'}
            ,params={'project_id':1031,'job_definition_id':job,'order_by':'-finished_at','limit':10}
            )
    data_list = res3.json().get('data')
    for data in data_list:
        if data['is_complete'] == data['is_success'] == True:
            runs.append(data['id'])
            finished_at.append(data['finished_at'])
            run_duration.append(data['run_duration'])
    job_index = jobs.index(job)
    final_job.append(job)
    final_job_name.append(job_names[job_index])

    if len(runs) > 0:
        latest_run = max(runs)
        latest_run_index = runs.index(latest_run)
        latest_finished_at = finished_at[latest_run_index]
        latest_run_duration = run_duration[latest_run_index]

        final_run.append(latest_run)
        final_finished_at.append(latest_finished_at)
        final_run_duration.append(latest_run_duration)
    else:
        final_run.append(np.nan)
        final_finished_at.append(np.nan)
        final_run_duration.append(np.nan)



job_run_dict = {
        'project_id':1031,
        'job_id':final_job,
        'job_name':final_job_name,
        'run_id':final_run,
        'finished_at':final_finished_at,
        'run_duration':final_run_duration
        }
job_run_df = pd.DataFrame(job_run_dict)
print(job_run_df)

# test: run id is correct, latest run should be mostly in this year

final_models= []
# testing case:
#final_run = [80027077]
for run in final_run:
    model_names = []
    compiled_path_list = []
    build_path_list = []
    print(run)
    if pd.isnull(run) == False:
        res = requests.get(
                    url=f"https://cloud.getdbt.com/api/v2/accounts/{ACCOUNT_ID}/runs/{run}/artifacts/manifest.json",
                    headers={'Authorization': f"Token {API_KEY}", 'Content-Type': 'application/json'},
                )
        data = res.json().get('nodes')
        for key in data.keys():
            position = list(data.keys()).index(key)
            values = list(data.values())[position]
            if 'compiled' in values.keys() and values['compiled'] == True:
                model_name = key.split('.')[-1]
                model_names.append(model_name)

    else:
        model_names.append(np.nan)

    final_models.append(model_names)

job_model = {
        'job_id':final_job,
        'job_name':final_job_name,
        'model_names':final_models
        }
job_model_df = pd.DataFrame(job_model).explode('model_names')
print(job_model_df)
#job_model_df.to_csv('job_model_lkp.csv',index=False)
