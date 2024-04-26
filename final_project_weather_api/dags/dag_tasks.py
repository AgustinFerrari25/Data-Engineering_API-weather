from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.models import Variable
import os
from airflow.models import XCom
from datetime import timedelta,datetime

from dag_functions import load_data, alert_mail
from dag_forecast_functions import get_data_forecast, get_db_connection_forecast
from dag_weather_functions import get_data_weather, get_db_connection_weather

column_name = Variable.get("COLUMN_NAME")
max_value = Variable.get("MAX_VALUE")
#b
dag_path = os.getcwd()
default_args={
    'owner':'Agustin',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay':timedelta(minutes=1)
}

dag= DAG(
    dag_id="final_project",
    schedule_interval="@daily",
    start_date=datetime(2024,4,17),
    catchup=False,
    default_args=default_args,
    tags=['weather']
)

pre_tasks= EmptyOperator(
    task_id='pre_tasks',
    dag=dag
)

task1_forecast = PythonOperator(
    task_id='get_data_forecast',
    python_callable=get_data_forecast,
    provide_context=True,
    op_kwargs={'dates': '{{ ds }}'},
    dag=dag
)

task2_forecast = PythonOperator(
    task_id='get_db_connection_forecast',
    python_callable=get_db_connection_forecast,
    provide_context=True,
    dag=dag
)

task3_forecast = PythonOperator(
    task_id='load_data_forecast',
    python_callable=load_data,
    provide_context=True,
    op_kwargs={'table_name': 'forecastday_per_hour', 'xcom_keys': ['df_forecast']},
    dag=dag
)

task4_forecast = PythonOperator(
    task_id='alert_mail_forecast',
    python_callable=alert_mail,
    provide_context=True,
    op_kwargs={'table_name': 'forecastday_per_hour', 'column_name': column_name, 'max_value': max_value},
    dag=dag
)

task1_weather = PythonOperator(
    task_id='get_data_weather',
    python_callable=get_data_weather,
    provide_context=True,
    op_kwargs={'dates': '{{ ds }}'},
    dag=dag
)

task2_weather = PythonOperator(
    task_id='get_db_connection_weather',
    python_callable=get_db_connection_weather,
    provide_context=True,
    dag=dag
)

task3_weather = PythonOperator(
    task_id='load_data_weather',
    python_callable=load_data,
    provide_context=True,
    op_kwargs={'table_name': 'weather_per_hour', 'xcom_keys': ['df_weather']},
    dag=dag
)

task4_weather = PythonOperator(
    task_id='alert_mail_weather',
    python_callable=alert_mail,
    provide_context=True,
    op_kwargs={'table_name': 'weather_per_hour', 'column_name': column_name, 'max_value': max_value},
    dag=dag
)

post_tasks= EmptyOperator(
    task_id='post_tasks',
    dag=dag
)

pre_tasks >> task1_forecast >> task2_forecast >> task3_forecast >> task4_forecast >> post_tasks 
pre_tasks >> task1_weather >> task2_weather >> task3_weather >> task4_weather >> post_tasks 