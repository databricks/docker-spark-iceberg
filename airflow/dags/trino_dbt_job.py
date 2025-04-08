"""
DAG for running dbt jobs with Trino as the SQL engine
"""
from datetime import datetime, timedelta
import os

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.trino.operators.trino import TrinoOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.models import Variable

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
    'trino_dbt_job',
    default_args=default_args,
    description='Run dbt transformations using Trino',
    schedule_interval=timedelta(days=1),
    catchup=False,
)

# Create a directory for our dbt project
def create_dbt_project():
    dbt_dir = '/opt/airflow/dags/dbt_project'
    if not os.path.exists(dbt_dir):
        os.makedirs(dbt_dir)
        os.makedirs(f"{dbt_dir}/models")
        os.makedirs(f"{dbt_dir}/macros")
    
    # Create dbt_project.yml
    with open(f"{dbt_dir}/dbt_project.yml", "w") as f:
        f.write("""
name: 'iceberg_transformations'
version: '1.0.0'
config-version: 2

profile: 'trino'

model-paths: ["models"]
macro-paths: ["macros"]

models:
  iceberg_transformations:
    +materialized: table
    """)
    
    # Create profiles.yml
    with open(f"{dbt_dir}/profiles.yml", "w") as f:
        f.write("""
trino:
  target: dev
  outputs:
    dev:
      type: trino
      method: none
      user: admin
      host: trino
      port: 8081
      catalog: iceberg
      schema: default
      threads: 1
      connector:
        type: trino
    """)
    
    # Create a simple model
    with open(f"{dbt_dir}/models/product_summary.sql", "w") as f:
        f.write("""
{{ config(materialized='table') }}

SELECT 
    name, 
    AVG(value) as avg_value, 
    MIN(value) as min_value, 
    MAX(value) as max_value,
    COUNT(*) as record_count
FROM {{ source('iceberg', 'airflow_spark_job') }}
GROUP BY name
    """)
    
    # Create schema.yml with source definition
    with open(f"{dbt_dir}/models/schema.yml", "w") as f:
        f.write("""
version: 2

sources:
  - name: iceberg
    database: iceberg
    schema: default
    tables:
      - name: airflow_spark_job
    """)
    
    return dbt_dir

# Create sample data in Trino
create_sample_table = TrinoOperator(
    task_id='create_sample_table',
    trino_conn_id='trino_default',
    sql="""
    CREATE TABLE IF NOT EXISTS iceberg.default.trino_sample_data (
        id INTEGER,
        name VARCHAR,
        value DOUBLE,
        processed_at VARCHAR
    )
    """,
    dag=dag,
)

# Insert sample data
insert_sample_data = TrinoOperator(
    task_id='insert_sample_data',
    trino_conn_id='trino_default',
    sql="""
    INSERT INTO iceberg.default.trino_sample_data
    VALUES
    (10, 'Product F', 60.5, current_timestamp),
    (11, 'Product G', 70.1, current_timestamp),
    (12, 'Product H', 80.9, current_timestamp)
    """,
    dag=dag,
)

# Create dbt project structure
create_dbt_dir = PythonOperator(
    task_id='create_dbt_project',
    python_callable=create_dbt_project,
    dag=dag,
)

# Run dbt
def run_dbt():
    import subprocess
    import os
    
    # Set up environment variables for dbt
    os.environ['DBT_PROFILES_DIR'] = '/opt/airflow/dags/dbt_project'
    
    # Run dbt commands
    subprocess.run(["dbt", "deps", "--project-dir", "/opt/airflow/dags/dbt_project"], check=True)
    subprocess.run(["dbt", "compile", "--project-dir", "/opt/airflow/dags/dbt_project"], check=True)
    result = subprocess.run(["dbt", "run", "--project-dir", "/opt/airflow/dags/dbt_project"], check=True)
    
    return result.returncode

run_dbt_task = PythonOperator(
    task_id='run_dbt',
    python_callable=run_dbt,
    dag=dag,
)

# Query the results of dbt transformations
query_results = TrinoOperator(
    task_id='query_dbt_results',
    trino_conn_id='trino_default',
    sql="""
    SELECT * FROM iceberg.default.product_summary
    """,
    dag=dag,
)

create_sample_table >> insert_sample_data >> create_dbt_dir >> run_dbt_task >> query_results 