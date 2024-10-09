from airflow import DAG
from functions.extract_data import get_airlines, save_to_supabase, log_dag_execution
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.dummy import DummyOperator
from datetime import datetime, timedelta

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

def check_none(**context):
    ti = context['ti']
    data = ti.xcom_pull(task_ids='get_airlines_task')
    #Using the task_id to determine the next step in the DAG based on the data being pulled from the get_airlines_task
    if data is None:
        return 'end_dag'
    else:
        return 'save_airlines_task'

with DAG(
    dag_id='get_airline_info',
    default_args=default_args,
    description='A simple DAG to extract airline information and save to Supabase',
    schedule_interval='@hourly',
    catchup=False #Catchup is set to False to prevent the DAG from running on all the previous dates
) as dag:
    
    task_get_airlines = PythonOperator(
        task_id='get_airlines_task',
        python_callable=get_airlines,
        op_kwargs={'airline_number': 'LH401'},
        do_xcom_push=True #Pushing the data to the next task
    )
    
    task_save_airlines = PythonOperator(
        task_id='save_airlines_task',
        python_callable=save_to_supabase,
        op_kwargs={'data': "{{ task_instance.xcom_pull(task_ids='get_airlines_task') }}"},
        provide_context=True #Providing the context to the next task
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

    check_data = BranchPythonOperator( #Being used to determine the next step in the DAG kind of like if else statement
        task_id='check_data',
        python_callable=check_none,
        provide_context=True
    )

    end_dag = DummyOperator(
        task_id='end_dag' #Ending the DAG
    )

    task_get_airlines >> start_log >> check_data
    check_data >> [task_save_airlines, end_dag]
    task_save_airlines >> end_log