import os
import pandas as pd
import sqlalchemy
from sqlalchemy import create_engine
from snowflake.sqlalchemy import URL

engine = create_engine(
        URL(account = os.getenv('SNOWFLAKE_ACCOUNT'), 
            user = os.getenv('SNOWFLAKE_USER'),
            password =os.getenv('SNOWFLAKE_PASSWORD'), 
            role = 'RL_DPT',
            warehouse = 'WH_DPT_XS',
            database = 'prod',
            schema = 'workspace_evelyn_chen'
            )
)

def upload_to_snowflake(
        dataframe,
        engine,
        table_name,
        truncate=False,
        create = True
        ):
    file_name = 'job_model_lkp.csv'
    file_path = os.path.abspath(file_name)

    with engine.connect() as con:
        print('connected')
        if create:
            dataframe.head(0).to_sql(
                    name=table_name,
                    con=con,
                    if_exists='replace',
                    index=False
                    )
        if truncate:
            con.execute(f'truncate table{table_name}')

        con.execute(f'PUT file:///{file_path}  @%{table_name}')
        con.execute(f'copy into {table_name}(job_id,job_name,model_name) from @%{table_name}')

df = pd.read_csv('job_model_lkp.csv')
upload_to_snowflake(df,engine,'job_model_lkp')
print(df)
