# Airflow Integration with Spark, Iceberg, and Trino

This guide explains how to use Airflow with Spark, Iceberg, and Trino to create and automate data workflows.

## Setup

1. Run the startup script to initialize all services:

```bash
./start-all.sh
```

This script:
- Creates necessary directories and configuration files
- Starts all containers (Spark, Iceberg, Trino, Minio, Airflow)
- Sets up Airflow connections for Trino and Spark
- Provides access information for all services

## Access Services

After starting, you can access the following services:

- **Airflow**: http://localhost:8090 (login: airflow/airflow)
- **Jupyter**: http://localhost:8888
- **Trino**: http://localhost:8085
- **Minio**: http://localhost:9001 (login: admin/password)

## Included DAGs

### 1. PySpark Iceberg Job (pyspark_iceberg_job)

This DAG:
- Creates a PySpark script at runtime
- Runs the script to create a sample Iceberg table
- Writes data to Minio-hosted Iceberg storage
- Verifies the data was written correctly

To run:
1. Go to Airflow UI
2. Navigate to DAGs
3. Find and trigger "pyspark_iceberg_job"

### 2. Trino DBT Job (trino_dbt_job)

This DAG:
- Creates a DBT project at runtime
- Creates a sample Trino table
- Inserts sample data
- Runs DBT to transform the data
- Queries the results of the transformation

To run:
1. Go to Airflow UI
2. Navigate to DAGs
3. Find and trigger "trino_dbt_job"

## Data Flow

The overall data flow is:

1. PySpark creates tables in Iceberg catalog stored in Minio
2. Trino reads these tables using the Iceberg connector
3. DBT transforms the data using Trino as the computation engine
4. Transformed data is written back to Iceberg catalog

## Verifying Results

To verify the results:

### In Minio:
1. Go to http://localhost:9001 and login with admin/password
2. Browse the "warehouse" bucket
3. Look for directories like "wh/default/airflow_spark_job" or "wh/default/product_summary"

### In Trino:
1. Connect to Trino via the Python client or UI
2. Run queries like:
   ```sql
   SELECT * FROM iceberg.default.airflow_spark_job
   SELECT * FROM iceberg.default.product_summary
   ```

### In Jupyter Notebook:
1. Access Jupyter at http://localhost:8888
2. Create a new notebook and use Trino or PySpark to query the tables:
   ```python
   import trino
   conn = trino.dbapi.connect(host="trino", port=8081, user="admin", catalog="iceberg", schema="default")
   cursor = conn.cursor()
   cursor.execute("SELECT * FROM product_summary")
   rows = cursor.fetchall()
   for row in rows:
       print(row)
   ```

## Important Note About Trino Port
Trino is configured to use port 8081 internally, not the default 8080. This is to avoid port conflicts with other services. Make sure to use port 8081 when:
- Creating connections to Trino
- Configuring DBT profiles
- Running direct Trino queries from Python or other tools

## Customizing DAGs

To create your own DAGs:
1. Add new DAG files to `airflow/dags/` directory
2. Use the existing DAGs as templates
3. Restart Airflow webserver to pick up the new DAGs

## Troubleshooting

- **Connection errors**: Check if all containers are running with `docker-compose ps`
- **Trino errors**: Check Trino logs with `docker-compose logs trino`
- **Airflow errors**: Check Airflow logs with `docker-compose logs airflow-webserver`
- **Missing tables**: Verify if table was created by querying Trino directly 
- **Trino Provider Missing**: If you encounter provider errors, run: `docker exec airflow-webserver python -m pip install apache-airflow-providers-trino`
- **Connection Failures**: If Trino connections fail, verify that the start-all.sh script completed successfully and the Trino service is running on port 8081 