import pytest
from dbt_api.dbt_api_poc import dbt_jobs

current_jobs = [1960,2325,2520,2521,2744,2933,2974,2982,3047,3121,3172,3368,3593,3702,3848,3892,3929,4116,5342,5385,5897,6069,6400,7220,7357,7903,8466,8765,8973,9829,9958,10304,10565,11193,12022,13239,13420,14140,14360,14547,14837,15412,15692,15904,17430,18251,18621,19135,19425,21408,21457,21550,21936,22907,22915,23032,23316,23692,25314,25563,26001,27412,27620,27761,28112,35413,38514,41238,45039,45250,46026,48444,55575,55973,56857,60431,75853,83192,87265,87318,87349,88158,91774,94719,95272,97549,97655,97804,103850,104049,117507,119776,120766,120941,122943]

# test first function can return value
def test_dbt_jobs_returns_value():
    response = dbt_jobs()
    assert type(response) != type(None)

# test first function can successfully call the api
# test first function returns a dict
def test_dbt_jobs_successful_api_call():
    response = dbt_jobs()
    assert type(response) == type(dict())

# test all the job ids have returned
def test_dbt_jobs_returns_all_jobs():
    job_set = dbt_jobs()
    assert len(job_set['job_id']) == len(current_jobs)
    assert job_set['job_id'] == current_jobs



