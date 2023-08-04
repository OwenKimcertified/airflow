from datetime import datetime

from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

default_args = {
  'start_date': datetime(2021, 1, 1),
}

with DAG(dag_id='taxi-price-pipeline',
         schedule_interval='@daily',
         default_args=default_args,
         tags=['spark'],
         catchup=False) as dag:
  
  preprocess = SparkSubmitOperator(
      application="dir", task_id="preprocess", conn_id="spark_local"
  )

  tune_hyperparameter = SparkSubmitOperator(
      application="dirt", task_id="tune_hyperparameter", conn_id="spark_local"
  )

  train_model = SparkSubmitOperator(
      application="dir", task_id="train_model", conn_id="spark_local"
  )

  preprocess >> tune_hyperparameter >> train_model