from datetime import datetime
from airflow import DAG
from airflow.providers.http.sensors.http import HttpSensor
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.operators.python import PythonOperator
from airflow.models import DagRun
from airflow.utils.session import create_session
from sqlalchemy import create_engine
# from airflow.operators.bash import BashOperator
# from pandas import json_normalize 보류
import pandas as pd, json, os


API_KEY = 'your key'

# airflow-connection Host : https://kr.api.riotgames.com/

arg = {'start_date' : datetime(2023, 8, 1)}
#------------------ mysql info

mysql_user = 'root'
mysql_password = 'root'
mysql_host = 'localhost'
mysql_port = '3306'
mysql_db = 'rbname'
conn_address = f"mysql+mysqlconnector://{mysql_user}:{mysql_password}@{mysql_host}:{mysql_port}/{mysql_db}"

#------------------- airflow info

dag_id = 'dagname'
execution_date_str = '2023-08-05T15:11:18.559982+00:00'
execution_date = datetime.strptime(execution_date_str, '%Y-%m-%dT%H:%M:%S.%f%z')
task_id = 'taskname'
xcom_key = 'return_value'


#------------------- 

def _processing_api():
    with create_session() as session:
        dagrun = session.query(DagRun).filter(
            DagRun.dag_id == dag_id,
            DagRun.execution_date == execution_date
        ).first()

    if not dagrun:
        raise Exception(f"DagRun for DAG '{dag_id}' and execution date '{execution_date}' not found.")
    
    else:
        xcom_value = dagrun.get_task_instance(task_id = task_id).xcom_pull(key = xcom_key)
        print(f"XCom Value: {xcom_value}")
    df = pd.DataFrame(xcom_value)
    df = df[['leagueId', 'summonerName', 'queueType']]
    print(df.head())

    dir = 'C://Users//admin//csv_//'  
    if not os.path.exists(dir):
        os.makedirs(dir)    
    
    file_path = os.path.join(dir, f'{datetime.now().date()}challenger_list.csv')
    df.to_csv(file_path)

    df.to_sql('user_info', con = create_engine(conn_address), if_exists = 'append', index = False)    


# dag skeleton
with DAG(dag_id = 'riot_datapipeline',
         schedule_interval = '@daily', # '0 0 * * * *'
         default_args = arg,
         tags = ['riot'],
         catchup = False) as dag: #catchup -> backfill

# operator
# bash func : bash operator
# python func : python operator
# email send : email operator

    api_check = HttpSensor(
        task_id = 'available_or_not',
        http_conn_id = 'riot_api',
        endpoint = f"lol/league-exp/v4/entries/RANKED_SOLO_5x5/CHALLENGER/I?page=1&api_key={API_KEY}"
    )

    extract_data = SimpleHttpOperator(
        task_id = 'extract_riot_api',
        http_conn_id = 'riot_api',
        endpoint = f"lol/league-exp/v4/entries/RANKED_SOLO_5x5/CHALLENGER/I?page=1&api_key={API_KEY}",
        method = 'GET',
        response_filter = lambda x: json.loads(x.text),
        log_response = True,
    )

    process_api_data = PythonOperator(
        task_id = 'process_api_data',
        python_callable = _processing_api,
        dag = dag
    )

    # store_data = BashOperator(
    #     task_id = 'store_data',
    #     bash_command = 'bash /path/to/your/bash_operator_script.sh'
    # )

api_check >> extract_data >> process_api_data