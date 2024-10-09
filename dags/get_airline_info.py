from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta
from functions.extract_data import get_airlines, save_to_supabase, log_dag_execution

default_args = {
    'owner': 'berwin_singh',
    'start_date': datetime(2024, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}
def log_dag_start(**context):
    dag_id = context['dag'].dag_id
    log_dag_execution(dag_id, 'started')

def log_dag_end(**context):
    dag_id = context['dag'].dag_id
    log_dag_execution(dag_id, 'completed')

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

    start_log = PythonOperator(
        task_id='start_log',
        python_callable=log_dag_start,
        provide_context=True
    )
    
    end_log = PythonOperator(
        task_id='end_log',
        python_callable=log_dag_end,
        provide_context=True
    )

    task_get_airlines >> start_log >> task_save_airlines >> task_hello >> end_log

# This line is just for testing, it won't affect the Airflow execution
print("DAG has been loaded")