"""
DAG for running PySpark jobs with Iceberg integration
"""
from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.operators.python import PythonOperator
from airflow.models import Variable
import os

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
    'pyspark_iceberg_job',
    default_args=default_args,
    description='PySpark job with Iceberg integration',
    schedule_interval=timedelta(days=1),
    catchup=False,
)

# Create a directory for our PySpark scripts if it doesn't exist
def create_pyspark_scripts_dir():
    scripts_dir = '/opt/airflow/dags/scripts'
    if not os.path.exists(scripts_dir):
        os.makedirs(scripts_dir)
    
    # Create our script file
    with open(f"{scripts_dir}/process_data.py", "w") as f:
        f.write("""
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType
from datetime import datetime

# Initialize Spark session with Iceberg and S3 support
spark = SparkSession.builder \\
    .appName("Airflow PySpark Job") \\
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \\
    .config("spark.sql.catalog.demo", "org.apache.iceberg.spark.SparkCatalog") \\
    .config("spark.sql.catalog.demo.type", "rest") \\
    .config("spark.sql.catalog.demo.uri", "http://rest:8181") \\
    .config("spark.sql.catalog.demo.io-impl", "org.apache.iceberg.aws.s3.S3FileIO") \\
    .config("spark.sql.catalog.demo.warehouse", "s3://warehouse/wh/") \\
    .config("spark.sql.catalog.demo.s3.endpoint", "http://minio:9000") \\
    .config("spark.sql.defaultCatalog", "demo") \\
    .getOrCreate()

print("Spark session created")

# Create sample data
schema = StructType([
    StructField("id", IntegerType(), False),
    StructField("name", StringType(), False),
    StructField("value", DoubleType(), False),
    StructField("processed_at", StringType(), False)
])

# Get current timestamp
current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

data = [
    (1, "Product A", 10.5, current_time),
    (2, "Product B", 20.1, current_time),
    (3, "Product C", 30.9, current_time),
    (4, "Product D", 40.5, current_time),
    (5, "Product E", 50.2, current_time)
]

print("Creating DataFrame")
df = spark.createDataFrame(data, schema)
df.show()

# Write to Iceberg table
print("Writing to Iceberg table")
table_name = "demo.default.airflow_spark_job"
df.writeTo(table_name).createOrReplace()

# Read back and validate
print("Reading back from Iceberg table")
result_df = spark.read.table(table_name)
count = result_df.count()
print(f"Total records: {count}")

print("Job completed successfully")
spark.stop()
        """)
    return scripts_dir

create_scripts_dir = PythonOperator(
    task_id='create_pyspark_scripts',
    python_callable=create_pyspark_scripts_dir,
    dag=dag,
)

# Run the PySpark job
spark_job = SparkSubmitOperator(
    task_id='spark_iceberg_job',
    application='/opt/airflow/dags/scripts/process_data.py',
    conn_id='spark_default',
    jars='/opt/spark/jars/iceberg-spark-runtime-3.5_2.12-1.8.1.jar,/opt/spark/jars/iceberg-aws-bundle-1.8.1.jar',
    conf={
        'spark.hadoop.fs.s3a.endpoint': 'http://minio:9000',
        'spark.hadoop.fs.s3a.access.key': 'admin',
        'spark.hadoop.fs.s3a.secret.key': 'password',
        'spark.hadoop.fs.s3a.path.style.access': 'true',
        'spark.hadoop.fs.s3a.impl': 'org.apache.hadoop.fs.s3a.S3AFileSystem',
    },
    verbose=True,
    dag=dag,
)

create_scripts_dir >> spark_job 