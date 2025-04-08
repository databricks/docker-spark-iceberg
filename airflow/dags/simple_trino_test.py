from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.trino.operators.trino import TrinoOperator

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'simple_trino_test',
    default_args=default_args,
    description='Simple Trino test DAG',
    schedule_interval=timedelta(days=1),
    catchup=False,
)

# Create a simple table
create_table = TrinoOperator(
    task_id='create_table',
    trino_conn_id='trino_default',
    sql="""
    CREATE TABLE IF NOT EXISTS iceberg.default.simple_test_table (
        id INTEGER,
        name VARCHAR,
        value DOUBLE
    )
    """,
    dag=dag,
)

# Insert data
insert_data = TrinoOperator(
    task_id='insert_data',
    trino_conn_id='trino_default',
    sql="""
    INSERT INTO iceberg.default.simple_test_table
    VALUES
    (1, 'Test A', 10.5),
    (2, 'Test B', 20.1),
    (3, 'Test C', 30.9)
    """,
    dag=dag,
)

# Query data
query_data = TrinoOperator(
    task_id='query_data',
    trino_conn_id='trino_default',
    sql="""
    SELECT * FROM iceberg.default.simple_test_table
    """,
    dag=dag,
)

create_table >> insert_data >> query_data 