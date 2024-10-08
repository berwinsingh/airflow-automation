from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta
from functions.extract_data import get_airlines, save_to_supabase
from airflow.utils.log.logging_mixin import LoggingMixin

default_args = {
    'owner': 'berwin_singh',
    'start_date': datetime(2024, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

def print_xcom(**context):
    ti = context['ti']
    airlines_data = ti.xcom_pull(task_ids='get_airlines_task')
    print(f"XCom data: {airlines_data}")
    return airlines_data

with DAG(
    dag_id='get_airline_info',
    default_args=default_args,
    description='A simple DAG to extract airline information and save to Supabase',
    schedule_interval='@hourly',
    catchup=False
) as dag:
    
    task_get_airlines = PythonOperator(
        task_id='get_airlines_task',
        python_callable=get_airlines,
        op_kwargs={'airline_number': 'LH401'},
        do_xcom_push=True
    )
    
    task_print_xcom = PythonOperator(
        task_id='print_xcom_task',
        python_callable=print_xcom,
        provide_context=True
    )
    
    task_save_airlines = PythonOperator(
        task_id='save_airlines_task',
        python_callable=save_to_supabase,
        op_kwargs={'data': "{{ task_instance.xcom_pull(task_ids='get_airlines_task') }}"},
        provide_context=True
    )
    
    task_hello = BashOperator(
        task_id='hello',
        bash_command='echo "Hello, this is a bash task"'
    )

    task_get_airlines >> task_print_xcom >> task_save_airlines >> task_hello

# This line is just for testing, it won't affect the Airflow execution
print("DAG has been loaded")