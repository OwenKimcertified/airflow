from datetime import datetime
from airflow import DAG
from airflow.providers.http.sensors.http import HttpSensor
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.operators.python import PythonOperator

from pandas import json_normalize
import json

API_KEY = 'RGAPI-5aac47c2-d310-4f44-82b9-e3861c97d43b'
# airflow-connection Host : https://kr.api.riotgames.com/

arg = {'start_date' : datetime(2023, 8, 1),\
       "provide_context" : True }

def _processing_api(ti):
    assets = ti.xcom_pull(task_ids = ['extract_riot_api']) # cross communication
    print(assets)
    # if not len(assets):
    #     raise ValueError('empty')
    # data = assets[0]
    # processed_assets = json_normalize({
    #     'summonerId' : data['summonerId'],
    #     'nickname' : data['summonerName'],
    #     'Qtype' : data['queueType']
    # })
    # processed_assets.to_csv('Users/admin/Desktop/test.csv', index = None, header = False)

# dag skeleton
with DAG(dag_id = 'riot_datapipeline',
         schedule_interval = '0 0 * * * *',
         default_args = arg,
         tags = ['riot'],
         catchup = False) as dag:

# operator
# bash func : bash operator
# python func : python operator
# email send : email operator

    api_check = HttpSensor(
        task_id = 'available_or_not',
        http_conn_id = 'riot_api',
        endpoint = 'api/v1/assets?collection=doodles-official&limit=1'
    )

    extract_data = SimpleHttpOperator(
        task_id = 'extract_riot_api',
        http_conn_id = 'riot_api',
        endpoint = f"lol/league-exp/v4/entries/RANKED_SOLO_5x5/CHALLENGER/I?page=1&api_key={API_KEY}",
        method = 'GET',
        response_filter = lambda x: json.loads(x.text),
        log_response = True
    )

    process_api_data = PythonOperator(
        task_id = 'process_api_data',
        python_callable = _processing_api
    )