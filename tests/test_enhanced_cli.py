import pytest
import time
import uuid
from unittest.mock import Mock, patch, MagicMock
from datetime import datetime, timedelta

from maestro.core.status_manager import StatusManager
from maestro.core.orchestrator import Orchestrator
from maestro.core.dag import DAG
from maestro.tasks.base import BaseTask


class SimpleTask(BaseTask):
    """Simple task for CLI testing."""
    message: str = "test"
    executed: bool = False
    
    def execute_local(self):
        self.executed = True


@pytest.fixture
def status_manager(tmp_path):
    """Create status manager with test database."""
    db_path = tmp_path / "test_cli.db"
    return StatusManager(str(db_path))


@pytest.fixture
def orchestrator_with_data(tmp_path):
    """Create orchestrator with test data."""
    db_path = tmp_path / "test_cli_orchestrator.db"
    orchestrator = Orchestrator(log_level="CRITICAL", db_path=str(db_path))
    orchestrator.register_task_type("simple_task", SimpleTask)
    return orchestrator


class TestStatusManagerCLIFeatures:
    """Test status manager features used by CLI."""
    
    def test_create_dag_execution(self, status_manager):
        """Test creating DAG execution records."""
        dag_id = "test_dag"
        execution_id = str(uuid.uuid4())
        
        with status_manager as sm:
            result = sm.create_dag_execution(dag_id, execution_id)
            assert result == execution_id
            
            # Verify execution was created
            details = sm.get_dag_execution_details(dag_id, execution_id)
            assert details["execution_id"] == execution_id
            assert details["status"] == "running"
            assert details["started_at"] is not None
    
    def test_update_dag_execution_status(self, status_manager):
        """Test updating DAG execution status."""
        dag_id = "test_dag"
        execution_id = str(uuid.uuid4())
        
        with status_manager as sm:
            sm.create_dag_execution(dag_id, execution_id)
            
            # Update status to completed
            sm.update_dag_execution_status(dag_id, execution_id, "completed")
            
            # Verify status update
            details = sm.get_dag_execution_details(dag_id, execution_id)
            assert details["status"] == "completed"
            assert details["completed_at"] is not None
    
    def test_get_running_dags(self, status_manager):
        """Test retrieving running DAGs."""
        dag_id1 = "running_dag_1"
        dag_id2 = "running_dag_2"
        dag_id3 = "completed_dag"
        
        with status_manager as sm:
            # Create running DAGs
            exec_id1 = sm.create_dag_execution(dag_id1, str(uuid.uuid4()))
            exec_id2 = sm.create_dag_execution(dag_id2, str(uuid.uuid4()))
            exec_id3 = sm.create_dag_execution(dag_id3, str(uuid.uuid4()))
            
            # Complete one DAG
            sm.update_dag_execution_status(dag_id3, exec_id3, "completed")
            
            # Get running DAGs
            running_dags = sm.get_running_dags()
            
            # Should only return running DAGs
            assert len(running_dags) == 2
            running_dag_ids = [dag["dag_id"] for dag in running_dags]
            assert dag_id1 in running_dag_ids
            assert dag_id2 in running_dag_ids
            assert dag_id3 not in running_dag_ids
    
    def test_get_dags_by_status(self, status_manager):
        """Test retrieving DAGs by specific status."""
        with status_manager as sm:
            # Create DAGs with different statuses
            dag_ids = []
            for i, status in enumerate(["running", "completed", "failed", "running"]):
                dag_id = f"dag_{i}"
                execution_id = str(uuid.uuid4())
                sm.create_dag_execution(dag_id, execution_id)
                if status != "running":
                    sm.update_dag_execution_status(dag_id, execution_id, status)
                dag_ids.append(dag_id)
            
            # Test getting running DAGs
            running_dags = sm.get_dags_by_status("running")
            assert len(running_dags) == 2
            
            # Test getting completed DAGs
            completed_dags = sm.get_dags_by_status("completed")
            assert len(completed_dags) == 1
            
            # Test getting failed DAGs
            failed_dags = sm.get_dags_by_status("failed")
            assert len(failed_dags) == 1
    
    def test_get_all_dags(self, status_manager):
        """Test retrieving all DAGs."""
        with status_manager as sm:
            # Create DAGs with different statuses
            dag_count = 5
            for i in range(dag_count):
                dag_id = f"dag_{i}"
                execution_id = str(uuid.uuid4())
                sm.create_dag_execution(dag_id, execution_id)
                if i % 2 == 0:
                    sm.update_dag_execution_status(dag_id, execution_id, "completed")
            
            # Get all DAGs
            all_dags = sm.get_all_dags()
            assert len(all_dags) == dag_count
            
            # Verify all have required fields
            for dag in all_dags:
                assert "dag_id" in dag
                assert "execution_id" in dag
                assert "status" in dag
                assert "started_at" in dag
    
    def test_get_dag_summary(self, status_manager):
        """Test getting DAG summary statistics."""
        with status_manager as sm:
            # Create DAGs with different statuses
            statuses = ["running", "completed", "failed", "completed", "running"]
            for i, status in enumerate(statuses):
                dag_id = f"dag_{i}"
                execution_id = str(uuid.uuid4())
                sm.create_dag_execution(dag_id, execution_id)
                if status != "running":
                    sm.update_dag_execution_status(dag_id, execution_id, status)
            
            # Get summary
            summary = sm.get_dag_summary()
            
            # Verify summary structure
            assert "total_executions" in summary
            assert "unique_dags" in summary
            assert "status_counts" in summary
            
            # Verify counts
            assert summary["total_executions"] == 5
            assert summary["unique_dags"] == 5
            assert summary["status_counts"]["running"] == 2
            assert summary["status_counts"]["completed"] == 2
            assert summary["status_counts"]["failed"] == 1
    
    def test_get_dag_history(self, status_manager):
        """Test getting DAG execution history."""
        dag_id = "test_dag"
        
        with status_manager as sm:
            # Create multiple executions for the same DAG
            execution_ids = []
            for i in range(3):
                execution_id = str(uuid.uuid4())
                sm.create_dag_execution(dag_id, execution_id)
                sm.update_dag_execution_status(dag_id, execution_id, "completed")
                execution_ids.append(execution_id)
            
            # Get history
            history = sm.get_dag_history(dag_id)
            
            # Verify history
            assert len(history) == 3
            for execution in history:
                assert execution["execution_id"] in execution_ids
                assert execution["status"] == "completed"
    
    def test_cleanup_old_executions(self, status_manager):
        """Test cleaning up old execution records."""
        with status_manager as sm:
            # Create some executions
            dag_id = "test_dag"
            execution_ids = []
            for i in range(5):
                execution_id = str(uuid.uuid4())
                sm.create_dag_execution(dag_id, execution_id)
                sm.update_dag_execution_status(dag_id, execution_id, "completed")
                execution_ids.append(execution_id)
            
            # Clean up all executions (0 days to keep)
            deleted_count = sm.cleanup_old_executions(0)
            
            # Verify cleanup
            assert deleted_count == 5
            
            # Verify all executions are gone
            all_dags = sm.get_all_dags()
            assert len(all_dags) == 0
    
    def test_cancel_dag_execution(self, status_manager):
        """Test cancelling DAG executions."""
        with status_manager as sm:
            # Create running executions
            dag_id = "test_dag"
            execution_id1 = str(uuid.uuid4())
            execution_id2 = str(uuid.uuid4())
            
            sm.create_dag_execution(dag_id, execution_id1)
            sm.create_dag_execution(dag_id, execution_id2)
            
            # Cancel specific execution
            success = sm.cancel_dag_execution(dag_id, execution_id1)
            assert success is True
            
            # Verify cancellation
            details = sm.get_dag_execution_details(dag_id, execution_id1)
            assert details["status"] == "cancelled"
            assert details["completed_at"] is not None
            
            # Other execution should still be running
            details2 = sm.get_dag_execution_details(dag_id, execution_id2)
            assert details2["status"] == "running"
    
    def test_log_message_and_retrieval(self, status_manager):
        """Test logging messages and retrieving them."""
        with status_manager as sm:
            dag_id = "test_dag"
            execution_id = str(uuid.uuid4())
            task_id = "test_task"

            sm.create_dag_execution(dag_id, execution_id)
    
            # Log some messages
            messages = [
                ("INFO", "Task started"),
                ("DEBUG", "Processing data"),
                ("INFO", "Task completed"),
                ("ERROR", "Something went wrong")
            ]
    
            for level, message in messages:
                sm.log_message(dag_id, execution_id, task_id, level, message)
    
            # Retrieve logs
            logs = sm.get_execution_logs(dag_id, execution_id)
    
            assert len(logs) == len(messages)
    
            # Verify messages (retrieved logs are in descending order)
            retrieved_messages = [log['message'] for log in reversed(logs)]
            original_messages = [msg[1] for msg in messages]
            assert retrieved_messages == original_messages
    
    def test_get_dag_execution_details(self, status_manager):
        """Test getting detailed DAG execution information."""
        with status_manager as sm:
            dag_id = "test_dag"
            execution_id = str(uuid.uuid4())
            
            # Create execution
            sm.create_dag_execution(dag_id, execution_id)
            
            # Add some task statuses
            sm.set_task_status(dag_id, "task1", "completed")
            sm.set_task_status(dag_id, "task2", "running")
            
            # Get details
            details = sm.get_dag_execution_details(dag_id, execution_id)
            
            # Verify details structure
            assert details["execution_id"] == execution_id
            assert details["status"] == "running"
            assert "started_at" in details
            assert "tasks" in details
            
            # Verify task details
            task_ids = [task["task_id"] for task in details["tasks"]]
            assert "task1" in task_ids
            assert "task2" in task_ids


class TestCLIIntegration:
    """Test CLI integration with orchestrator and status manager."""
    
    def test_orchestrator_run_dag_in_thread_cli_integration(self, orchestrator_with_data):
        """Test run_dag_in_thread for CLI integration."""
        # Create simple DAG
        dag = DAG(dag_id="cli_test_dag")
        task = SimpleTask(task_id="simple_task", message="Hello CLI")
        dag.add_task(task)
        
        # Run in thread (simulating CLI async execution)
        execution_id = orchestrator_with_data.run_dag_in_thread(dag)
        
        # Wait for completion
        time.sleep(0.2)
        
        # Verify execution was tracked
        with orchestrator_with_data.status_manager as sm:
            details = sm.get_dag_execution_details(dag.dag_id, execution_id)
            assert details["execution_id"] == execution_id
            assert details["status"] == "completed"

    def test_dag_status_monitoring_cli_scenario(self, orchestrator_with_data):
        """Test DAG status monitoring scenario for CLI."""
        dag = DAG(dag_id="monitor_dag")
        task = SimpleTask(task_id="monitor_task", message="Monitor me")
        dag.add_task(task)
        
        # Start execution
        execution_id = orchestrator_with_data.run_dag_in_thread(dag)
        
        # Wait for completion first
        time.sleep(0.3)
        
        # Monitor execution (simulating CLI monitor command) - use separate context
        with orchestrator_with_data.status_manager as sm:
            # Check final status
            details = sm.get_dag_execution_details(dag.dag_id, execution_id)
            assert details["status"] == "completed"
    
    def test_resume_functionality_cli_scenario(self, orchestrator_with_data):
        """Test resume functionality for CLI."""
        dag = DAG(dag_id="resume_dag")
        task1 = SimpleTask(task_id="task1", message="First task")
        task2 = SimpleTask(task_id="task2", message="Second task", dependencies=["task1"])
        dag.add_task(task1)
        dag.add_task(task2)
        
        # Simulate partial execution
        with orchestrator_with_data.status_manager as sm:
            sm.set_task_status(dag.dag_id, "task1", "completed")
            sm.set_task_status(dag.dag_id, "task2", "pending")
        
        # Resume execution
        execution_id = orchestrator_with_data.run_dag_in_thread(dag, resume=True)
        
        # Wait for completion
        time.sleep(0.2)
        
        # Verify resume worked
        with orchestrator_with_data.status_manager as sm:
            details = sm.get_dag_execution_details(dag.dag_id, execution_id)
            assert details["status"] == "completed"
            
            # task1 should not have been executed again
            assert not dag.tasks["task1"].executed
            # task2 should have been executed
            assert dag.tasks["task2"].executed


class TestCLIDataStructures:
    """Test data structures and formats expected by CLI."""
    
    def test_dag_execution_details_format(self, status_manager):
        """Test that DAG execution details have expected format for CLI."""
        with status_manager as sm:
            dag_id = "format_test_dag"
            execution_id = str(uuid.uuid4())
            
            # Create execution with tasks
            sm.create_dag_execution(dag_id, execution_id)
            sm.set_task_status(dag_id, "task1", "completed")
            sm.set_task_status(dag_id, "task2", "running")
            
            # Get details
            details = sm.get_dag_execution_details(dag_id, execution_id)
            
            # Verify CLI-expected format
            required_fields = ["execution_id", "status", "started_at", "completed_at", "thread_id", "pid", "tasks"]
            for field in required_fields:
                assert field in details
            
            # Verify task format
            assert len(details["tasks"]) == 2
            for task in details["tasks"]:
                task_fields = ["task_id", "status", "started_at", "completed_at", "thread_id"]
                for field in task_fields:
                    assert field in task
    
    def test_logs_format_for_cli(self, status_manager):
        """Test that logs have expected format for CLI display."""
        with status_manager as sm:
            dag_id = "log_format_dag"
            execution_id = str(uuid.uuid4())

            sm.create_dag_execution(dag_id, execution_id)
    
            # Log some messages
            sm.log_message(dag_id, execution_id, "task1", "INFO", "Test message")
    
            logs = sm.get_execution_logs(dag_id, execution_id)
            assert len(logs) == 1
            log_entry = logs[0]
    
            expected_keys = ["task_id", "level", "message", "timestamp", "thread_id"]
            for key in expected_keys:
                assert key in log_entry
                
            # Verify log levels are valid
            assert log_entry["level"] in ["DEBUG", "INFO", "WARNING", "ERROR"]
    
    def test_summary_format_for_cli(self, status_manager):
        """Test that summary has expected format for CLI display."""
        with status_manager as sm:
            # Create some test data
            for i in range(5):
                dag_id = f"summary_dag_{i}"
                execution_id = str(uuid.uuid4())
                sm.create_dag_execution(dag_id, execution_id)
                if i % 2 == 0:
                    sm.update_dag_execution_status(dag_id, execution_id, "completed")
            
            # Get summary
            summary = sm.get_dag_summary()
            
            # Verify CLI-expected format
            required_fields = ["total_executions", "unique_dags", "status_counts"]
            for field in required_fields:
                assert field in summary
            
            # Verify status_counts is a dictionary
            assert isinstance(summary["status_counts"], dict)
            assert summary["total_executions"] == 5
            assert summary["unique_dags"] == 5
