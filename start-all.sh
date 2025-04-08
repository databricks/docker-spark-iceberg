#!/bin/bash
set -e

echo "Starting all services..."

# Create necessary directories
mkdir -p airflow/dags airflow/logs airflow/plugins airflow/dags/scripts airflow/dags/dbt_project
mkdir -p dev/trino/etc/catalog

# Create Trino config files
cat > dev/trino/etc/node.properties << EOF
node.environment=production
node.id=trino-1
node.data-dir=/data/trino
EOF

cat > dev/trino/etc/config.properties << EOF
coordinator=true
node-scheduler.include-coordinator=true
http-server.http.port=8081
discovery-server.enabled=true
discovery.uri=http://trino:8081
EOF

cat > dev/trino/etc/jvm.config << EOF
-server
-Xmx4G
-XX:+UseG1GC
-XX:G1HeapRegionSize=32M
-XX:+ExplicitGCInvokesConcurrent
-XX:+HeapDumpOnOutOfMemoryError
-XX:+ExitOnOutOfMemoryError
-Djdk.attach.allowAttachSelf=true
-Djdk.nio.maxCachedBufferSize=2000000
EOF

cat > dev/trino/etc/catalog/iceberg.properties << EOF
connector.name=iceberg
iceberg.catalog.type=rest
iceberg.rest-catalog.uri=http://rest:8181
hive.s3.aws-access-key=admin
hive.s3.aws-secret-key=password
hive.s3.endpoint=http://minio:9000
hive.s3.path-style-access=true
EOF

# Start the containers
echo "Starting Docker containers..."
docker-compose down
docker-compose up -d

# Wait for services to be available
echo "Waiting for services to be available..."
sleep 30

# Install requirements for Airflow
echo "Installing Airflow requirements..."
docker exec airflow-webserver python -m pip install -r /opt/airflow/requirements.txt --no-cache-dir

# Create default schema in Trino
echo "Creating default schema in Trino..."
docker exec trino trino --server localhost:8081 --catalog iceberg --execute "CREATE SCHEMA IF NOT EXISTS default"

# Install Trino provider for Airflow
echo "Ensuring Trino provider is installed..."
docker exec airflow-webserver python -m pip install apache-airflow-providers-trino
docker exec airflow-webserver python -m pip install apache-airflow-providers-apache-spark
docker exec airflow-webserver python -m pip install 'apache-airflow-providers-openlineage>=1.8.0'

# Check if Python is available
if ! command -v python &> /dev/null; then
    if command -v python3 &> /dev/null; then
        echo "Python command not found, but python3 is available. Using python3..."
        alias python=python3
    else
        echo "Error: Neither python nor python3 command found. Please install Python 3 before continuing."
        exit 1
    fi
fi

# Set up Airflow connections
echo "Setting up Airflow connections..."

# Check if Trino connection already exists
CONNECTION_EXISTS=$(docker exec airflow-webserver airflow connections get trino_default 2>/dev/null || echo "NOT_FOUND")

if [[ $CONNECTION_EXISTS == *"NOT_FOUND"* ]]; then
    echo "Trino connection does not exist, creating..."
    
    # Try running the setup script first
    if command -v python &> /dev/null && python -m pip install requests &> /dev/null; then
        echo "Using local Python installation to set up Airflow connections..."
        python airflow/setup_connections.py
    else
        # Fallback to running the setup script in the Airflow container
        echo "Local Python setup failed. Using Docker to set up Airflow connections..."
        # Copy the setup script to the Airflow container
        docker cp airflow/setup_connections.py airflow-webserver:/tmp/setup_connections.py
        
        # Run the setup script in the Airflow container
        docker exec airflow-webserver bash -c "python /tmp/setup_connections.py"
        
        # If the script fails, set up the connection manually
        if [ $? -ne 0 ]; then
            echo "Setting up Trino connection manually..."
            docker exec airflow-webserver airflow connections add trino_default --conn-type trino --conn-host trino --conn-schema default --conn-login admin --conn-port 8081 --conn-extra '{"catalog": "iceberg"}'
        fi
    fi
else
    echo "Trino connection already exists."
fi

# Test the Trino connection
echo "Testing Trino connection..."
docker exec airflow-webserver python -c "from airflow.providers.trino.hooks.trino import TrinoHook; hook = TrinoHook('trino_default'); print(f'Connected to Trino host: {hook.get_conn().host}'); print(f'Test query result: {hook.get_records(\"SELECT 1\")}');" || echo "Trino connection test failed, but continuing..."

# Restart Airflow services
echo "Restarting Airflow services to load updated packages..."
docker restart airflow-webserver
docker restart airflow-scheduler

# Wait for Airflow to be available again
echo "Waiting for Airflow services to restart..."
sleep 15

echo "All services are up and running!"
echo ""
echo "Access points:"
echo "- Airflow: http://localhost:8090 (airflow/airflow)"
echo "- Jupyter: http://localhost:8888"
echo "- Trino: http://localhost:8085"
echo "- Minio: http://localhost:9001 (admin/password)"
echo ""
echo "To trigger DAGs in Airflow:"
echo "1. Go to http://localhost:8090"
echo "2. Login with airflow/airflow"
echo "3. Navigate to DAGs and trigger the 'pyspark_iceberg_job' or 'trino_dbt_job'"
echo ""
echo "To check data in Minio:"
echo "1. Go to http://localhost:9001"
echo "2. Login with admin/password"
echo "3. Browse the 'warehouse' bucket to see the Iceberg tables"