#!/usr/bin/env python3

import requests
import time

base_url = "http://localhost:8000"

# Create a DAG
response = requests.post(f"{base_url}/v1/dags/create", json={
    "dag_file_path": "./examples/sample_dag.yaml",
    "dag_id": "test_run_manual"
})
print(f"Create response: {response.status_code} - {response.json()}")

# Run the DAG
response = requests.post(f"{base_url}/v1/dags/test_run_manual/run", json={
    "dag_id": "test_run_manual"
})
print(f"Run response: {response.status_code} - {response.json()}")

time.sleep(2)

# Check status
response = requests.get(f"{base_url}/v1/dags/test_run_manual/status")
print(f"Status response: {response.status_code} - {response.json()}")

# List all DAGs
response = requests.get(f"{base_url}/v1/dags?filter=all")
print(f"List all DAGs: {response.status_code} - {response.json()}")

# List active DAGs
response = requests.get(f"{base_url}/v1/dags?filter=active")
print(f"List active DAGs: {response.status_code} - {response.json()}")
