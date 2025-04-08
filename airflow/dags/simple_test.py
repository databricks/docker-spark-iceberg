from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

def print_hello():
    print('Hello from Airflow!')
    with open('/tmp/hello.txt', 'w') as f:
        f.write('Hello, Airflow created this file!')
    return 'Hello'

dag = DAG(
    'simple_test',
    default_args=default_args,
    description='Simple test DAG',
    schedule_interval=timedelta(days=1),
    catchup=False,
)

hello_task = PythonOperator(
    task_id='print_hello',
    python_callable=print_hello,
    dag=dag,
) 