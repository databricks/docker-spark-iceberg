#!/usr/bin/env python
"""
Setup script to create necessary Airflow connections
"""
import time
import requests
import json
import os
import sys
from urllib.parse import urljoin

# Wait for Airflow webserver to be ready
def wait_for_airflow(base_url, max_retries=30, retry_interval=5):
    """Wait for Airflow webserver to be ready"""
    print("Waiting for Airflow webserver...")
    for i in range(max_retries):
        try:
            response = requests.get(urljoin(base_url, "/health"))
            if response.status_code == 200:
                print("Airflow webserver is ready!")
                return True
        except requests.exceptions.RequestException:
            pass
        
        print(f"Airflow not ready yet, retrying in {retry_interval} seconds... ({i+1}/{max_retries})")
        time.sleep(retry_interval)
    
    print("Timed out waiting for Airflow webserver")
    return False

# Create Airflow connections
def create_connections(base_url, username, password):
    """Create the necessary Airflow connections"""
    auth = (username, password)
    headers = {'Content-Type': 'application/json'}
    
    # Trino connection
    trino_conn = {
        "connection_id": "trino_default",
        "conn_type": "trino",
        "host": "trino",
        "port": 8081,
        "login": "admin",
        "schema": "default",
        "extra": json.dumps({
            "catalog": "iceberg"
        })
    }
    
    # Spark connection
    spark_conn = {
        "connection_id": "spark_default",
        "conn_type": "spark",
        "host": "spark-iceberg",
        "port": 7077,
        "extra": json.dumps({
            "deploy-mode": "client"
        })
    }
    
    # Create connections
    for conn in [trino_conn, spark_conn]:
        conn_id = conn["connection_id"]
        print(f"Creating connection: {conn_id}")
        
        # Check if connection exists
        response = requests.get(
            urljoin(base_url, f"api/v1/connections/{conn_id}"),
            auth=auth,
            headers=headers
        )
        
        # If connection exists, delete it
        if response.status_code == 200:
            print(f"Connection {conn_id} already exists, deleting...")
            requests.delete(
                urljoin(base_url, f"api/v1/connections/{conn_id}"),
                auth=auth,
                headers=headers
            )
        
        # Create connection
        response = requests.post(
            urljoin(base_url, "api/v1/connections"),
            auth=auth,
            headers=headers,
            json=conn
        )
        
        if response.status_code == 200:
            print(f"Connection {conn_id} created successfully!")
        else:
            print(f"Failed to create connection {conn_id}: {response.status_code} {response.text}")

def main():
    # Check if running inside Docker container
    in_docker = os.path.exists('/.dockerenv')
    
    # If inside Docker, use the internal webserver address
    if in_docker:
        base_url = "http://localhost:8080/"
        print("Running inside Docker container, using internal address:", base_url)
    else:
        base_url = "http://localhost:8090/"
        print("Running outside Docker container, using external address:", base_url)
    
    username = "airflow"
    password = "airflow"
    
    # Wait for Airflow to be ready
    if not wait_for_airflow(base_url):
        sys.exit(1)
    
    # Create connections
    create_connections(base_url, username, password)
    
    print("Setup completed successfully!")

if __name__ == "__main__":
    main() 