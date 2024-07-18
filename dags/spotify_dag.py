from datetime import timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from datetime import datetime
from spotify_etl import run_spotify_etl

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

def run_spotify_etl2():
    print("This is another ETL process")

dag = DAG(
    'spotify_dag',
    default_args=default_args,
    description='A simple ETL process for Spotify data',
    schedule_interval=timedelta(days=1),
    start_date=days_ago(1),
    tags=['example'],
)

run_etl = PythonOperator(
    task_id='whole_spotify_etl',
    python_callable=run_spotify_etl,
    dag=dag,
)

run_etl2 = PythonOperator(
    task_id='whole_spotify_etl2',
    python_callable= run_spotify_etl2,
    dag=dag,
)

run_etl >> run_etl2