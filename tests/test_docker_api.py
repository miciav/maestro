import pytest
from fastapi.testclient import TestClient
from maestro.server.app import app
from maestro.core.status_manager import StatusManager
import os
import time

@pytest.fixture(scope="module")
def client():
    # Use a test database for isolation
    test_db_path = "./test_maestro_docker.db"
    if os.path.exists(test_db_path):
        os.remove(test_db_path)
    
    # Import the app first to trigger the lifespan context manager
    from maestro.server.app import app
    # Import docker_api to set the orchestrator
    from maestro.server import docker_api
    from maestro.core.orchestrator import Orchestrator
    
    # Create an orchestrator instance for testing
    test_orchestrator = Orchestrator(log_level="INFO", db_path=test_db_path)
    test_orchestrator.disable_rich_logging()
    
    # Override the orchestrator in docker_api module
    docker_api.orchestrator = test_orchestrator
    
    with TestClient(app) as c:
        yield c
    
    # Clean up test database
    if os.path.exists(test_db_path):
        os.remove(test_db_path)

@pytest.fixture(scope="module")
def sample_dag_file():
    # Use a simple test DAG that doesn't require external resources
    return os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "examples", "simple_test_dag.yaml"))

def test_create_dag(client: TestClient, sample_dag_file: str):
    response = client.post(
        "/v1/dags/create",
        json={
            "dag_file_path": sample_dag_file,
            "dag_id": "test_dag_create"
        }
    )
    assert response.status_code == 200
    assert response.json()["dag_id"] == "test_dag_create"
    assert "created successfully" in response.json()["message"]

    # Test creating with auto-generated ID
    response = client.post(
        "/v1/dags/create",
        json={
            "dag_file_path": sample_dag_file
        }
    )
    assert response.status_code == 200
    assert response.json()["dag_id"] is not None
    assert "created successfully" in response.json()["message"]

def test_create_dag_duplicate_id(client: TestClient, sample_dag_file: str):
    client.post(
        "/v1/dags/create",
        json={
            "dag_file_path": sample_dag_file,
            "dag_id": "duplicate_dag"
        }
    )
    response = client.post(
        "/v1/dags/create",
        json={
            "dag_file_path": sample_dag_file,
            "dag_id": "duplicate_dag"
        }
    )
    assert response.status_code == 400
    assert "already exists" in response.json()["detail"]

def test_run_dag(client: TestClient, sample_dag_file: str):
    # First, create the DAG
    create_response = client.post(
        "/v1/dags/create",
        json={
            "dag_file_path": sample_dag_file,
            "dag_id": "test_dag_run"
        }
    )
    assert create_response.status_code == 200

    # Then, run the DAG
    run_response = client.post(
        "/v1/dags/test_dag_run/run",
        json={
            "dag_id": "test_dag_run"
        }
    )
    assert run_response.status_code == 200
    assert run_response.json()["dag_id"] == "test_dag_run"
    assert run_response.json()["execution_id"] is not None
    assert run_response.json()["status"] == "submitted"

    # Wait for DAG to complete (adjust time as needed for sample_dag.yaml)
    time.sleep(5) 

    # Check status
    status_response = client.get(f"/dags/test_dag_run/status")
    assert status_response.status_code == 200
    assert status_response.json()["status"] == "completed"

def test_log_dag(client: TestClient, sample_dag_file: str):
    # Create and run a DAG first
    client.post("/v1/dags/create", json={"dag_file_path": sample_dag_file, "dag_id": "test_dag_log"})
    run_response = client.post("/v1/dags/test_dag_log/run", json={"dag_id": "test_dag_log"})
    print(f"\n[DEBUG] Run response: {run_response.status_code} - {run_response.json()}")
    execution_id = run_response.json()["execution_id"]
    time.sleep(5) # Wait for logs to be generated

    response = client.get(f"/v1/dags/test_dag_log/log?execution_id={execution_id}")
    assert response.status_code == 200
    logs = response.json()
    assert len(logs) > 0
    assert any("task1" in log["task_id"] for log in logs)

""" def test_attach_dag(client: TestClient, sample_dag_file: str):
    # Create and run a DAG first
    client.post("/v1/dags/create", json={"dag_file_path": sample_dag_file, "dag_id": "test_dag_attach"})
    client.post("/v1/dags/test_dag_attach/run", json={"dag_id": "test_dag_attach"})

    # Stream logs
    messages = []
    with client.stream("GET", "/v1/dags/test_dag_attach/attach") as response:
        for chunk in response.iter_bytes():
            messages.append(chunk.decode("utf-8"))
            if "Task 'task2' completed" in chunk.decode("utf-8"):
                break
    
    assert any("task1" in msg for msg in messages)
    assert any("task3" in msg for msg in messages) """

def test_rm_dag(client: TestClient, sample_dag_file: str):
    # Create a DAG
    client.post("/v1/dags/create", json={"dag_file_path": sample_dag_file, "dag_id": "test_dag_rm"})
    
    # Remove the DAG
    response = client.delete("/v1/dags/test_dag_rm")
    assert response.status_code == 200
    assert "removed successfully" in response.json()["message"]

    # Verify it's gone
    response = client.get("/v1/dags?filter=all")
    assert response.status_code == 200
    assert not any(d["dag_id"] == "test_dag_rm" for d in response.json())

def test_rm_running_dag_force(client: TestClient, sample_dag_file: str):
    # Create and run a DAG
    client.post("/v1/dags/create", json={"dag_file_path": sample_dag_file, "dag_id": "test_dag_rm_force"})
    client.post("/v1/dags/test_dag_rm_force/run", json={"dag_id": "test_dag_rm_force"})
    time.sleep(1) # Give it a moment to start running

    # Try to remove without force (should fail)
    response = client.delete("/v1/dags/test_dag_rm_force")
    assert response.status_code == 409 # Conflict
    assert "currently running" in response.json()["detail"]

    # Remove with force
    response = client.delete("/v1/dags/test_dag_rm_force?force=true")
    assert response.status_code == 200
    assert "removed successfully" in response.json()["message"]

    # Verify it's gone
    response = client.get("/v1/dags?filter=all")
    assert response.status_code == 200
    assert not any(d["dag_id"] == "test_dag_rm_force" for d in response.json())

def test_ls_dags(client: TestClient, sample_dag_file: str):
    # Create a few DAGs for testing ls
    client.post("/v1/dags/create", json={"dag_file_path": sample_dag_file, "dag_id": "ls_dag_1"})
    client.post("/v1/dags/create", json={"dag_file_path": sample_dag_file, "dag_id": "ls_dag_2"})
    client.post("/v1/dags/ls_dag_1/run", json={"dag_id": "ls_dag_1"})
    time.sleep(1) # Give it a moment to start running

    # Test ls all
    response = client.get("/v1/dags?filter=all")
    assert response.status_code == 200
    dags = response.json()
    print(f"\n[DEBUG] All DAGs response: {dags}")
    assert any(d["dag_id"] == "ls_dag_1" for d in dags)
    assert any(d["dag_id"] == "ls_dag_2" for d in dags)

    # Test ls active
    response = client.get("/v1/dags?filter=active")
    assert response.status_code == 200
    dags = response.json()
    print(f"\n[DEBUG] Active DAGs response: {dags}")
    assert any(d["dag_id"] == "ls_dag_1" and d["status"] == "running" for d in dags)
    assert not any(d["dag_id"] == "ls_dag_2" for d in dags) # ls_dag_2 is not running

    # Test ls terminated (after ls_dag_1 completes)
    time.sleep(5) # Wait for ls_dag_1 to complete
    response = client.get("/v1/dags?filter=terminated")
    assert response.status_code == 200
    dags = response.json()
    assert any(d["dag_id"] == "ls_dag_1" and d["status"] == "completed" for d in dags)
    assert not any(d["dag_id"] == "ls_dag_2" for d in dags) # ls_dag_2 was never run

def test_stop_dag(client: TestClient, sample_dag_file: str):
    # Create and run a DAG
    client.post("/v1/dags/create", json={"dag_file_path": sample_dag_file, "dag_id": "test_dag_stop"})
    run_response = client.post("/v1/dags/test_dag_stop/run", json={"dag_id": "test_dag_stop"})
    execution_id = run_response.json()["execution_id"]
    time.sleep(1) # Give it a moment to start running

    # Stop the DAG
    response = client.post(f"/v1/dags/test_dag_stop/stop?execution_id={execution_id}")
    assert response.status_code == 200
    assert "Successfully stopped" in response.json()["message"]

    # Verify status is cancelled
    status_response = client.get(f"/dags/test_dag_stop/status?execution_id={execution_id}")
    assert status_response.status_code == 200
    assert status_response.json()["status"] == "cancelled"

def test_resume_dag(client: TestClient, sample_dag_file: str):
    # Create and run a DAG, then stop it
    client.post("/v1/dags/create", json={"dag_file_path": sample_dag_file, "dag_id": "test_dag_resume"})
    run_response = client.post("/v1/dags/test_dag_resume/run", json={"dag_id": "test_dag_resume"})
    execution_id = run_response.json()["execution_id"]
    time.sleep(1) # Give it a moment to start running
    client.post(f"/v1/dags/test_dag_resume/stop?execution_id={execution_id}")
    time.sleep(1) # Give it a moment to stop

    # Resume the DAG
    response = client.post(f"/v1/dags/test_dag_resume/resume?execution_id={execution_id}")
    assert response.status_code == 200
    assert "resumed with new execution ID" in response.json()["message"]

    # Verify a new execution is running
    time.sleep(5) # Wait for it to complete
    status_response = client.get(f"/dags/test_dag_resume/status")
    assert status_response.status_code == 200
    assert status_response.json()["status"] == "completed"
