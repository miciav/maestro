#!/usr/bin/env python3
"""Test script to verify DAG status returns all tasks in all states"""

import requests
import json
import time
import sys

# API base URL
BASE_URL = "http://localhost:8000"

def test_dag_status():
    # Create a simple test DAG file
    test_dag_content = """
dag_id: test_status_dag
tasks:
  - task_id: task1
    type: PrintTask
    message: "Task 1"
  
  - task_id: task2
    type: PrintTask
    message: "Task 2"
    dependencies: [task1]
  
  - task_id: task3
    type: PrintTask
    message: "Task 3"
    dependencies: [task1]
  
  - task_id: task4
    type: PrintTask
    message: "Task 4"
    dependencies: [task2, task3]
"""
    
    # Write test DAG to file
    with open("/tmp/test_status_dag.yaml", "w") as f:
        f.write(test_dag_content)
    
    print("1. Submitting DAG...")
    # Submit the DAG
    submit_response = requests.post(
        f"{BASE_URL}/dags/submit",
        json={
            "dag_file_path": "/tmp/test_status_dag.yaml",
            "fail_fast": False
        }
    )
    
    if submit_response.status_code != 200:
        print(f"Failed to submit DAG: {submit_response.text}")
        return
    
    submit_data = submit_response.json()
    dag_id = submit_data["dag_id"]
    execution_id = submit_data["execution_id"]
    
    print(f"DAG submitted: {dag_id}, execution: {execution_id}")
    
    # Check status immediately (should show all tasks as pending)
    print("\n2. Checking status immediately after submission...")
    status_response = requests.get(f"{BASE_URL}/dags/{dag_id}/status?execution_id={execution_id}")
    
    if status_response.status_code == 200:
        status_data = status_response.json()
        print(f"Execution status: {status_data['status']}")
        print(f"Number of tasks: {len(status_data['tasks'])}")
        
        # Group tasks by status
        tasks_by_status = {}
        for task in status_data['tasks']:
            status = task['status']
            if status not in tasks_by_status:
                tasks_by_status[status] = []
            tasks_by_status[status].append(task['task_id'])
        
        print("\nTasks by status:")
        for status, task_ids in tasks_by_status.items():
            print(f"  {status}: {task_ids}")
    
    # Wait a bit and check again
    print("\n3. Waiting 2 seconds and checking again...")
    time.sleep(2)
    
    status_response = requests.get(f"{BASE_URL}/dags/{dag_id}/status?execution_id={execution_id}")
    
    if status_response.status_code == 200:
        status_data = status_response.json()
        print(f"Execution status: {status_data['status']}")
        print(f"Number of tasks: {len(status_data['tasks'])}")
        
        # Group tasks by status
        tasks_by_status = {}
        for task in status_data['tasks']:
            status = task['status']
            if status not in tasks_by_status:
                tasks_by_status[status] = []
            tasks_by_status[status].append(task['task_id'])
        
        print("\nTasks by status:")
        for status, task_ids in tasks_by_status.items():
            print(f"  {status}: {task_ids}")
        
        # Print detailed task info
        print("\nDetailed task information:")
        for task in status_data['tasks']:
            print(f"  - {task['task_id']}: {task['status']}")
            if task.get('started_at'):
                print(f"    Started: {task['started_at']}")
            if task.get('completed_at'):
                print(f"    Completed: {task['completed_at']}")

if __name__ == "__main__":
    try:
        test_dag_status()
    except Exception as e:
        print(f"Error: {e}")
        sys.exit(1)
