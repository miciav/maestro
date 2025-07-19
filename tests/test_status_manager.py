import pytest
import threading
import uuid
from maestro.core.status_manager import StatusManager
from datetime import datetime, timedelta

@pytest.fixture
def status_manager():
    """Create a fresh in-memory StatusManager for each test."""
    # Use a unique database path for each test to avoid shared state
    import tempfile
    import os
    temp_file = tempfile.NamedTemporaryFile(delete=False)
    temp_file.close()
    
    sm = StatusManager(temp_file.name)
    yield sm
    
    # Clean up the temporary file
    try:
        os.unlink(temp_file.name)
    except (FileNotFoundError, PermissionError):
        pass


def test_set_and_get_task_status(status_manager):
    dag_id = "dag_123"
    task_id = "task_1"

    with status_manager as sm:
        # Set status without execution_id (should create default execution)
        sm.set_task_status(dag_id, task_id, "completed")
        status = sm.get_task_status(dag_id, task_id)
        assert status == "completed"

        # Set status with specific execution_id
        execution_id = "exec_456"
        # Create execution first
        sm.create_dag_execution(dag_id, execution_id)
        sm.set_task_status(dag_id, task_id, "running", execution_id)
        status = sm.get_task_status(dag_id, task_id, execution_id)
        assert status == "running"


def test_get_dag_status(status_manager):
    dag_id = "dag_123"
    execution_id = "exec_456"

    with status_manager as sm:
        # Create execution before setting task status
        sm.create_dag_execution(dag_id, execution_id)
        sm.set_task_status(dag_id, "task_1", "completed", execution_id)
        sm.set_task_status(dag_id, "task_2", "running", execution_id)

        dag_status = sm.get_dag_status(dag_id, execution_id)
        assert dag_status["task_1"] == "completed"
        assert dag_status["task_2"] == "running"


def test_reset_dag_status(status_manager):
    dag_id = "dag_123"
    execution_id = "exec_456"

    with status_manager as sm:
        # Create execution before setting task status
        sm.create_dag_execution(dag_id, execution_id)
        sm.set_task_status(dag_id, "task_1", "completed", execution_id)
        sm.reset_dag_status(dag_id, execution_id)

        dag_status = sm.get_dag_status(dag_id, execution_id)
        assert not dag_status


def test_create_dag_execution(status_manager):
    dag_id = "dag_123"
    execution_id = "exec_456"

    with status_manager as sm:
        sm.create_dag_execution(dag_id, execution_id)
        execution_details = sm.get_dag_execution_details(dag_id, execution_id)
        assert execution_details["execution_id"] == execution_id


def test_logging_and_retrieval(status_manager):
    dag_id = "dag_123"
    execution_id = "exec_456"
    task_id = "task_1"

    with status_manager as sm:
        # Create execution before logging
        sm.create_dag_execution(dag_id, execution_id)
        sm.log_message(dag_id, execution_id, task_id, "INFO", "Test log message.")
        logs = sm.get_execution_logs(dag_id, execution_id)

        assert len(logs) == 1
        assert logs[0]["message"] == "Test log message."


def test_cleanup_old_executions(status_manager):
    dag_id = "dag_123"
    old_execution_id = "old_exec"
    new_execution_id = "new_exec"
    old_date = datetime.now().replace(year=2000).isoformat()

    with status_manager as sm:
        # Create old execution by direct SQL insertion
        sm._conn.execute("""
            INSERT OR IGNORE INTO dags (id) VALUES (?)
        """, (dag_id,))
        sm._conn.execute("""
            INSERT INTO executions (id, dag_id, started_at, status, thread_id, pid)
            VALUES (?, ?, ?, ?, ?, ?)""",
            (old_execution_id, dag_id, old_date, "completed", str(threading.get_ident()), str(threading.get_ident()))
        )
        
        # Create new execution
        sm.create_dag_execution(dag_id, new_execution_id)

        # Clean up executions older than a year
        cleaned = sm.cleanup_old_executions(days_to_keep=365)

        assert cleaned == 1
        executions = sm.get_dag_history(dag_id)
        assert len(executions) == 1
        assert executions[0]["execution_id"] == new_execution_id


def test_get_task_status_fallback(status_manager):
    """Test the fallback logic for getting task status."""
    dag_id = "dag_123"
    task_id = "task_1"
    execution_id = "exec_456"

    with status_manager as sm:
        # Set task status without specific execution_id (uses "default")
        sm.set_task_status(dag_id, task_id, "completed")
        
        # Create a new execution but don't set task status for it
        sm.create_dag_execution(dag_id, execution_id)
        
        # Should fallback to default execution when task not found for specific execution
        status = sm.get_task_status(dag_id, task_id, execution_id)
        assert status == "completed"


def test_get_running_dags(status_manager):
    """Test getting all running DAGs."""
    dag_id1 = "dag_1"
    dag_id2 = "dag_2"
    execution_id1 = "exec_1"
    execution_id2 = "exec_2"

    with status_manager as sm:
        # Create running executions
        sm.create_dag_execution(dag_id1, execution_id1)
        sm.create_dag_execution(dag_id2, execution_id2)
        
        # Mark one as completed
        sm.update_dag_execution_status(dag_id1, execution_id1, "completed")
        
        running_dags = sm.get_running_dags()
        assert len(running_dags) == 1
        assert running_dags[0]["dag_id"] == dag_id2
        assert running_dags[0]["execution_id"] == execution_id2


def test_get_dags_by_status(status_manager):
    """Test getting DAGs by status."""
    dag_id1 = "dag_1"
    dag_id2 = "dag_2"
    execution_id1 = "exec_1"
    execution_id2 = "exec_2"

    with status_manager as sm:
        # Create executions
        sm.create_dag_execution(dag_id1, execution_id1)
        sm.create_dag_execution(dag_id2, execution_id2)
        
        # Update statuses
        sm.update_dag_execution_status(dag_id1, execution_id1, "completed")
        sm.update_dag_execution_status(dag_id2, execution_id2, "failed")
        
        completed_dags = sm.get_dags_by_status("completed")
        failed_dags = sm.get_dags_by_status("failed")
        
        assert len(completed_dags) == 1
        assert completed_dags[0]["dag_id"] == dag_id1
        
        assert len(failed_dags) == 1
        assert failed_dags[0]["dag_id"] == dag_id2


def test_get_all_dags(status_manager):
    """Test getting all DAGs."""
    dag_id1 = "dag_1"
    dag_id2 = "dag_2"
    execution_id1 = "exec_1"
    execution_id2 = "exec_2"

    with status_manager as sm:
        sm.create_dag_execution(dag_id1, execution_id1)
        sm.create_dag_execution(dag_id2, execution_id2)
        
        all_dags = sm.get_all_dags()
        assert len(all_dags) == 2
        
        dag_ids = [dag["dag_id"] for dag in all_dags]
        assert dag_id1 in dag_ids
        assert dag_id2 in dag_ids


def test_get_dag_summary(status_manager):
    """Test getting DAG summary statistics."""
    with status_manager as sm:
        # Create multiple executions with different statuses
        for i in range(3):
            dag_id = f"dag_{i}"
            execution_id = f"exec_{i}"
            sm.create_dag_execution(dag_id, execution_id)
            
            if i == 0:
                sm.update_dag_execution_status(dag_id, execution_id, "completed")
            elif i == 1:
                sm.update_dag_execution_status(dag_id, execution_id, "failed")
            # i == 2 remains "running"
        
        summary = sm.get_dag_summary()
        
        assert summary["total_executions"] == 3
        assert summary["unique_dags"] == 3
        assert summary["status_counts"]["completed"] == 1
        assert summary["status_counts"]["failed"] == 1
        assert summary["status_counts"]["running"] == 1


def test_cancel_dag_execution(status_manager):
    """Test cancelling a DAG execution."""
    dag_id = "dag_123"
    execution_id = "exec_456"

    with status_manager as sm:
        sm.create_dag_execution(dag_id, execution_id)
        
        # Cancel the execution
        cancelled = sm.cancel_dag_execution(dag_id, execution_id)
        assert cancelled is True
        
        # Check the status was updated
        details = sm.get_dag_execution_details(dag_id, execution_id)
        assert details["status"] == "cancelled"
        assert details["completed_at"] is not None


def test_mark_incomplete_tasks_as_failed(status_manager):
    """Test marking incomplete tasks as failed."""
    dag_id = "dag_123"
    execution_id = "exec_456"

    with status_manager as sm:
        sm.create_dag_execution(dag_id, execution_id)
        
        # Set various task statuses
        sm.set_task_status(dag_id, "task_1", "running", execution_id)
        sm.set_task_status(dag_id, "task_2", "pending", execution_id)
        sm.set_task_status(dag_id, "task_3", "completed", execution_id)
        
        # Mark incomplete tasks as failed
        sm.mark_incomplete_tasks_as_failed(dag_id, execution_id)
        
        # Check results
        assert sm.get_task_status(dag_id, "task_1", execution_id) == "failed"
        assert sm.get_task_status(dag_id, "task_2", execution_id) == "failed"
        assert sm.get_task_status(dag_id, "task_3", execution_id) == "completed"  # Should remain unchanged


def test_initialize_tasks_for_execution(status_manager):
    """Test initializing tasks for an execution."""
    dag_id = "dag_123"
    execution_id = "exec_456"
    task_ids = ["task_1", "task_2", "task_3"]

    with status_manager as sm:
        sm.create_dag_execution(dag_id, execution_id)
        sm.initialize_tasks_for_execution(dag_id, execution_id, task_ids)
        
        # All tasks should be pending
        for task_id in task_ids:
            status = sm.get_task_status(dag_id, task_id, execution_id)
            assert status == "pending"


def test_task_status_with_timestamps(status_manager):
    """Test that task status updates include proper timestamps."""
    dag_id = "dag_123"
    execution_id = "exec_456"
    task_id = "task_1"

    with status_manager as sm:
        sm.create_dag_execution(dag_id, execution_id)
        
        # Set to running (should set started_at)
        sm.set_task_status(dag_id, task_id, "running", execution_id)
        
        # Set to completed (should set completed_at)
        sm.set_task_status(dag_id, task_id, "completed", execution_id)
        
        # Get execution details to check timestamps
        details = sm.get_dag_execution_details(dag_id, execution_id)
        task = next(t for t in details["tasks"] if t["task_id"] == task_id)
        
        assert task["started_at"] is not None
        assert task["completed_at"] is not None
        assert task["status"] == "completed"

