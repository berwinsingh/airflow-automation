from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from datetime import datetime
from functions.extract_data import get_airlines
from datetime import timedelta

def extract_airline_info():
    airlines = get_airlines()
    print(airlines)
    return airlines

default_args = {
    'owner': 'berwin_singh',
    'start_date': datetime(2024, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='get_airline_info',
    default_args=default_args,
    description='A simple DAG to extract airline information',
    schedule_interval='@daily',
    catchup=False
) as dag:
    extract_airline_info = PythonOperator(
        task_id='extract_airline_info',
        python_callable=extract_airline_info,
    )

    extract_airline_info >> BashOperator(
        task_id='hello',
        bash_command="echo hello"
    )