#!/bin/bash
set -e

echo "Setting up the entire stack..."

# Step 1: Create custom Airflow Dockerfile directory
mkdir -p airflow/docker

# Step 2: Create a custom Dockerfile for Airflow
cat > airflow/docker/Dockerfile << EOF
FROM apache/airflow:2.8.1

USER root
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    && apt-get autoremove -yqq --purge \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*
USER airflow

COPY requirements.txt /requirements.txt
RUN pip install --no-cache-dir -r /requirements.txt
EOF

# Step 3: Copy requirements to Docker build context
cp airflow/requirements.txt airflow/docker/

# Step 4: Build custom Airflow image
echo "Building custom Airflow image with dependencies..."
docker build -t airflow-custom:2.8.1 airflow/docker/

# Step 5: Temporarily modify docker-compose.yml to use custom image
echo "Updating docker-compose.yml to use custom image..."
sed -i.bak 's|image: apache/airflow:2.8.1|image: airflow-custom:2.8.1|g' docker-compose.yml

# Step 6: Run the original start-all.sh script
echo "Running start-all.sh..."
./start-all.sh

# Step 7: Restore original docker-compose.yml (optional, only if needed)
# mv docker-compose.yml.bak docker-compose.yml

echo "Setup completed successfully!"