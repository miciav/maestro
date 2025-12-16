
import pytest
from fastapi.testclient import TestClient
from unittest.mock import patch, MagicMock
import os
import sys
from datetime import datetime
from contextlib import asynccontextmanager

# Add the src directory to the Python path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../src')))

from maestro.server.app import app, lifespan
from maestro.shared.dag import DAG
from maestro.shared.task import Task

# Create a concrete implementation of the abstract Task class for testing purposes
class ConcreteTask(Task):
    def execute_local(self, **kwargs):
        # This method is abstract in the base class, so we provide a minimal implementation.
        print(f"Executing task {self.name} with action: {self.action}")
        return f"Output of {self.name}"

# Sample DAG for testing
@pytest.fixture
def sample_dag():
    task1 = ConcreteTask(task_id="task1", name="Task 1", action="echo 'Task 1'", dependencies=[])
    task2 = ConcreteTask(task_id="task2", name="Task 2", action="echo 'Task 2'", dependencies=["task1"])
    dag = DAG(dag_id="sample_dag")
    dag.add_task(task1)
    dag.add_task(task2)
    return dag

@pytest.fixture
def client(sample_dag):
    # This fixture provides a test client for the FastAPI app.
    # It patches the global `orchestrator` object that is used by the API endpoints.
    
    # 1. Create a mock for the Orchestrator.
    mock_orchestrator = MagicMock()
    mock_orchestrator.load_dag_from_file.return_value = sample_dag
    mock_orchestrator.run_dag_in_thread.return_value = "test_execution_id"
    
    # 2. The original `lifespan` function creates a real Orchestrator. We need to prevent that.
    # We replace the app's lifespan with a dummy one for the tests.
    @asynccontextmanager
    async def mock_lifespan(app):
        # This context manager does nothing, preventing the real orchestrator creation.
        yield

    app.router.lifespan_context = mock_lifespan

    # 3. Patch the global `orchestrator` variable in the `app` module with our mock.
    # This is the instance that the endpoint functions will actually use.
    with patch('maestro.server.app.orchestrator', mock_orchestrator):
        with TestClient(app) as test_client:
            yield test_client


def test_root(client):
    response = client.get("/")
    assert response.status_code == 200
    json_response = response.json()
    assert json_response["message"] == "Maestro API Server"
    assert json_response["status"] == "running"

def test_submit_dag_success(client, sample_dag):
    with patch('maestro.server.app.orchestrator.load_dag_from_file',
               return_value=sample_dag):
        with patch('maestro.server.app.orchestrator.run_dag_in_thread',
                   return_value="test_execution_id") as mock_run:
            response = client.post("/dags/submit",
                                   json={"dag_file_path": "path/to/dag.yaml",
                                         "resume": False,
                                         "fail_fast": True})
            
            assert response.status_code == 200
            json_response = response.json()
            assert json_response["dag_id"] == "sample_dag"
            assert json_response["execution_id"] == "test_execution_id"
            assert json_response["status"] == "submitted"
            assert "submitted_at" in json_response
            mock_run.assert_called_once_with(dag=sample_dag,
                                             resume=False,
                                             fail_fast=True)

def test_submit_dag_file_not_found(client):
    with patch('maestro.server.app.orchestrator.load_dag_from_file', side_effect=FileNotFoundError("DAG file not found")):
        response = client.post("/dags/submit", json={"dag_file_path": "non_existent.yaml"})
        assert response.status_code == 400
        assert "DAG file not found" in response.json()["detail"]

def test_get_dag_status_success(client):
    mock_status_manager = MagicMock()
    mock_status_manager.get_dag_execution_details.return_value = {
        "execution_id": "test_execution_id",
        "status": "running",
        "started_at": datetime.now().isoformat(),
        "completed_at": None,
        "thread_id": "thread_123",
        "tasks": []
    }
    with patch('maestro.server.app.orchestrator.status_manager', __enter__=MagicMock(return_value=mock_status_manager)):
        response = client.get("/dags/sample_dag/status?execution_id=test_execution_id")
        assert response.status_code == 200
        json_response = response.json()
        assert json_response["dag_id"] == "sample_dag"
        assert json_response["execution_id"] == "test_execution_id"
        assert json_response["status"] == "running"

def test_get_dag_status_server_error(client):
    with patch('maestro.server.app.orchestrator.status_manager', __enter__=MagicMock(side_effect=Exception("DB error"))):
        response = client.get("/dags/sample_dag/status")
        assert response.status_code == 500
        assert "DB error" in response.json()["detail"]

def test_get_dag_status_not_found(client):
    mock_status_manager = MagicMock()
    mock_status_manager.get_dag_execution_details.return_value = None
    with patch('maestro.server.app.orchestrator.status_manager', __enter__=MagicMock(return_value=mock_status_manager)):
        response = client.get("/dags/unknown_dag/status")
        assert response.status_code == 404
        assert "DAG execution not found" in response.json()["detail"]

def test_get_dag_logs_success(client):
    mock_logs = [
        {"task_id": "task1", "level": "INFO", "message": "Task 1 started", "timestamp": datetime.now().isoformat(), "thread_id": "thread_123"}
    ]
    mock_status_manager = MagicMock()
    mock_status_manager.get_execution_logs.return_value = mock_logs
    with patch('maestro.server.app.orchestrator.status_manager', __enter__=MagicMock(return_value=mock_status_manager)):
        response = client.get("/dags/sample_dag/logs")
        assert response.status_code == 200
        json_response = response.json()
        assert json_response["dag_id"] == "sample_dag"
        assert len(json_response["logs"]) == 1
        assert json_response["logs"][0]["task_id"] == "task1"

import asyncio

def test_get_dag_logs_with_filters(client):
    mock_logs = [
        {"task_id": "task1", "level": "INFO", "message": "Task 1 started", "timestamp": datetime.now().isoformat(), "thread_id": "thread_123"},
        {"task_id": "task2", "level": "DEBUG", "message": "Task 2 debug", "timestamp": datetime.now().isoformat(), "thread_id": "thread_123"}
    ]
    mock_status_manager = MagicMock()
    mock_status_manager.get_execution_logs.return_value = mock_logs
    with patch('maestro.server.app.orchestrator.status_manager', __enter__=MagicMock(return_value=mock_status_manager)):
        response = client.get("/dags/sample_dag/logs?task_filter=task1&level_filter=INFO")
        assert response.status_code == 200
        json_response = response.json()
        assert len(json_response["logs"]) == 1
        assert json_response["logs"][0]["task_id"] == "task1"
        assert json_response["logs"][0]["level"] == "INFO"


@pytest.mark.asyncio
async def test_stream_dag_logs(client):
    mock_status_manager = MagicMock()
    logs_queue = asyncio.Queue()

    async def mock_get_logs(*args, **kwargs):
        try:
            log = await asyncio.wait_for(logs_queue.get(), timeout=1.0)
            return [log]
        except asyncio.TimeoutError:
            return []

    mock_status_manager.get_execution_logs.side_effect = mock_get_logs

    with patch('maestro.server.app.orchestrator.status_manager', __enter__=MagicMock(return_value=mock_status_manager)):
        response = client.get("/dags/sample_dag/logs/stream")
        assert response.status_code == 200

        # Simulate adding a log entry
        log_entry = {"task_id": "task1", "level": "INFO", "message": "A new log", "timestamp": datetime.now().isoformat(), "thread_id": "thread_123"}
        await logs_queue.put(log_entry)

        # The client-side implementation to read from the stream would be more complex.
        # For this test, we are mainly ensuring the endpoint can be hit and returns a streaming response.
        # A more thorough test would involve a client that can handle server-sent events.

def test_get_dag_logs_server_error(client):
    with patch('maestro.server.app.orchestrator.status_manager', __enter__=MagicMock(side_effect=Exception("DB error"))):
        response = client.get("/dags/sample_dag/logs")
        assert response.status_code == 500
        assert "DB error" in response.json()["detail"]


def test_get_running_dags_empty(client):
    mock_status_manager = MagicMock()
    mock_status_manager.get_running_dags.return_value = []
    with patch('maestro.server.app.orchestrator.status_manager', __enter__=MagicMock(return_value=mock_status_manager)):
        response = client.get("/dags/running")
        assert response.status_code == 200
        json_response = response.json()
        assert json_response["count"] == 0
        assert json_response["running_dags"] == []

def test_get_running_dags(client):
    running_dags = [{"dag_id": "dag1", "execution_id": "exec1"}]
    mock_status_manager = MagicMock()
    mock_status_manager.get_running_dags.return_value = running_dags
    with patch('maestro.server.app.orchestrator.status_manager', __enter__=MagicMock(return_value=mock_status_manager)):
        response = client.get("/dags/running")
        assert response.status_code == 200
        json_response = response.json()
        assert json_response["count"] == 1
        assert json_response["running_dags"][0]["dag_id"] == "dag1"

def test_get_running_dags_server_error(client):
    with patch('maestro.server.app.orchestrator.status_manager', __enter__=MagicMock(side_effect=Exception("DB error"))):
        response = client.get("/dags/running")
        assert response.status_code == 500
        assert "DB error" in response.json()["detail"]

def test_list_dags_all(client):
    all_dags = [{"dag_id": "dag1", "status": "completed"}, {"dag_id": "dag2", "status": "failed"}]
    mock_status_manager = MagicMock()
    mock_status_manager.get_all_dags.return_value = all_dags
    with patch('maestro.server.app.orchestrator.status_manager', __enter__=MagicMock(return_value=mock_status_manager)):
        response = client.get("/dags/list")
        assert response.status_code == 200
        json_response = response.json()
        assert json_response["count"] == 2
        assert json_response["title"] == "All DAGs"

def test_list_dags_with_status_filter(client):
    completed_dags = [{"dag_id": "dag1", "status": "completed"}]
    mock_status_manager = MagicMock()
    mock_status_manager.get_dags_by_status.return_value = completed_dags
    with patch('maestro.server.app.orchestrator.status_manager', __enter__=MagicMock(return_value=mock_status_manager)):
        response = client.get("/dags/list?status=completed")
        assert response.status_code == 200
        json_response = response.json()
        assert json_response["count"] == 1
        assert json_response["title"] == "DAGs with status: completed"
        mock_status_manager.get_dags_by_status.assert_called_once_with("completed")

def test_list_dags_server_error(client):
    with patch('maestro.server.app.orchestrator.status_manager', __enter__=MagicMock(side_effect=Exception("DB error"))):
        response = client.get("/dags/list")
        assert response.status_code == 500
        assert "DB error" in response.json()["detail"]

def test_cancel_dag_success(client):
    mock_status_manager = MagicMock()
    mock_status_manager.cancel_dag_execution.return_value = True
    with patch('maestro.server.app.orchestrator.status_manager', __enter__=MagicMock(return_value=mock_status_manager)):
        response = client.post("/dags/sample_dag/cancel?execution_id=exec1")
        assert response.status_code == 200
        assert response.json()["success"] is True
        mock_status_manager.cancel_dag_execution.assert_called_once_with("sample_dag", "exec1")

def test_cancel_dag_not_found(client):
    mock_status_manager = MagicMock()
    mock_status_manager.cancel_dag_execution.return_value = False
    with patch('maestro.server.app.orchestrator.status_manager', __enter__=MagicMock(return_value=mock_status_manager)):
        response = client.post("/dags/sample_dag/cancel")
        assert response.status_code == 200
        assert response.json()["success"] is False

def test_cancel_dag_server_error(client):
    with patch('maestro.server.app.orchestrator.status_manager', __enter__=MagicMock(side_effect=Exception("DB error"))):
        response = client.post("/dags/sample_dag/cancel")
        assert response.status_code == 500
        assert "DB error" in response.json()["detail"]

def test_validate_dag_success(client, sample_dag):
    with patch('maestro.server.app.orchestrator.load_dag_from_file', return_value=sample_dag):
        response = client.post("/dags/validate", json={"dag_file_path": "path/to/dag.yaml"})
        assert response.status_code == 200
        json_response = response.json()
        assert json_response["valid"] is True
        assert json_response["dag_id"] == "sample_dag"
        assert json_response["total_tasks"] == 2
        assert json_response["execution_order"] == ["task1", "task2"]

def test_validate_dag_invalid(client):
    with patch('maestro.server.app.orchestrator.load_dag_from_file', side_effect=Exception("Invalid DAG")):
        response = client.post("/dags/validate", json={"dag_file_path": "path/to/invalid_dag.yaml"})
        assert response.status_code == 200
        json_response = response.json()
        assert json_response["valid"] is False
        assert "Invalid DAG" in json_response["error"]

def test_validate_dag_missing_path(client):
    response = client.post("/dags/validate", json={})
    assert response.status_code == 200
    json_response = response.json()
    assert json_response["valid"] is False
    assert "dag_file_path is required" in json_response["error"]

def test_cleanup_old_executions(client):
    mock_status_manager = MagicMock()
    mock_status_manager.cleanup_old_executions.return_value = 5
    with patch('maestro.server.app.orchestrator.status_manager', __enter__=MagicMock(return_value=mock_status_manager)):
        response = client.delete("/dags/cleanup?days=15")
        assert response.status_code == 200
        json_response = response.json()
        assert json_response["deleted_count"] == 5
        assert "Deleted 5 execution records older than 15 days" in json_response["message"]
        mock_status_manager.cleanup_old_executions.assert_called_once_with(15)

def test_cleanup_old_executions_server_error(client):
    with patch('maestro.server.app.orchestrator.status_manager', __enter__=MagicMock(side_effect=Exception("DB error"))):
        response = client.delete("/dags/cleanup")
        assert response.status_code == 500
        assert "DB error" in response.json()["detail"]

def test_main_start_server():
    with patch('maestro.server.app.uvicorn.run') as mock_run:
        with patch('argparse.ArgumentParser') as mock_argparse:
            mock_argparse.return_value.parse_args.return_value = MagicMock(host="127.0.0.1", port=8080, log_level="debug")
            from maestro.server.app import main
            main()
            mock_run.assert_called_once()


@pytest.mark.asyncio
async def test_lifespan(monkeypatch):
    mock_orchestrator = MagicMock()
    monkeypatch.setattr("maestro.server.app.Orchestrator", lambda **kwargs: mock_orchestrator)

    # Import the app module to access the orchestrator variable
    from maestro.server import app as app_module

    async with lifespan(app):
        # Access the orchestrator from the app module, not as a global in test scope
        assert app_module.orchestrator is not None

    mock_orchestrator.executor.shutdown.assert_called_once_with(wait=True)



