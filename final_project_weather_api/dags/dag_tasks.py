from airflow import DAG
from airflow.operators.python import PythonOperator
import os
from datetime import timedelta,datetime
from dag_funciones import get_data, get_db_connection, load_data

dag_path = os.getcwd()
default_args={
    'owner':'Agustin',
    'retries': 1,
    'retry_delay':timedelta(minutes=5)
}

dag= DAG(
    dag_id="final_project",
    schedule_interval="@daily",
    start_date=datetime(2024,4,1),
    catchup=False,
    default_args=default_args
)

task1 = PythonOperator(
    task_id='get_data',
    python_callable=get_data,
    dag=dag
)

task2 = PythonOperator(
    task_id='get_db_connection',
    python_callable=get_db_connection,
    dag=dag
)

task3 = PythonOperator(
    task_id='load_data',
    python_callable=load_data,
    dag=dag
)

task1 >> task2 >> task3