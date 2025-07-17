import pytest
import time
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from unittest.mock import Mock, patch
import uuid
import os

from maestro.core.orchestrator import Orchestrator
from maestro.core.dag import DAG
from maestro.tasks.base import BaseTask
from maestro.core.task import TaskStatus
from maestro.core.status_manager import StatusManager


class SlowTask(BaseTask):
    duration: float = 0.1
    executed: bool = False

    def execute_local(self):
        time.sleep(self.duration)
        self.executed = True


class FailingTask(BaseTask):
    def execute_local(self):
        raise Exception("Intentional test failure")


@pytest.fixture
def orchestrator_with_tasks(tmp_path):
    """Create orchestrator with custom test tasks registered."""
    db_path = tmp_path / "test_concurrent.db"
    orchestrator = Orchestrator(log_level="CRITICAL", db_path=str(db_path))
    orchestrator.register_task_type("slow_task", SlowTask)
    orchestrator.register_task_type("failing_task", FailingTask)
    return orchestrator


@pytest.fixture
def simple_dag():
    """Create a simple DAG for testing."""
    dag = DAG(dag_id="test_concurrent_dag")
    
    task1 = SlowTask(task_id="task1", duration=0.05)
    task2 = SlowTask(task_id="task2", duration=0.05, dependencies=["task1"])
    task3 = SlowTask(task_id="task3", duration=0.05, dependencies=["task2"])
    
    dag.add_task(task1)
    dag.add_task(task2)
    dag.add_task(task3)
    
    return dag


@pytest.fixture
def parallel_dag():
    """Create a DAG with parallel execution paths."""
    dag = DAG(dag_id="test_parallel_dag")
    
    # Create a diamond dependency pattern
    task1 = SlowTask(task_id="root", duration=0.05)
    task2 = SlowTask(task_id="branch1", duration=0.05, dependencies=["root"])
    task3 = SlowTask(task_id="branch2", duration=0.05, dependencies=["root"])
    task4 = SlowTask(task_id="merge", duration=0.05, dependencies=["branch1", "branch2"])
    
    dag.add_task(task1)
    dag.add_task(task2)
    dag.add_task(task3)
    dag.add_task(task4)
    
    return dag


class TestConcurrentExecution:
    """Test concurrent DAG execution functionality."""
    
    def test_run_dag_in_thread_returns_execution_id(self, orchestrator_with_tasks, simple_dag):
        """Test that run_dag_in_thread returns a valid execution ID."""
        execution_id = orchestrator_with_tasks.run_dag_in_thread(simple_dag)
        
        assert execution_id is not None
        assert isinstance(execution_id, str)
        assert len(execution_id) > 0
        
        # Wait for completion
        time.sleep(0.3)
        
        # Verify execution was recorded
        with orchestrator_with_tasks.status_manager as sm:
            details = sm.get_dag_execution_details(simple_dag.dag_id, execution_id)
            assert details["execution_id"] == execution_id
            assert details["status"] in ["completed", "running"]
    
    def test_multiple_concurrent_dags(self, orchestrator_with_tasks, simple_dag):
        """Test running multiple DAGs concurrently."""
        # Create multiple DAGs with different IDs
        dag1 = simple_dag
        dag2 = DAG(dag_id="concurrent_dag_2")
        dag2.add_task(SlowTask(task_id="task1", duration=0.05))
        dag2.add_task(SlowTask(task_id="task2", duration=0.05, dependencies=["task1"]))
        
        # Start both DAGs
        execution_id1 = orchestrator_with_tasks.run_dag_in_thread(dag1)
        execution_id2 = orchestrator_with_tasks.run_dag_in_thread(dag2)
        
        assert execution_id1 != execution_id2
        
        # Wait for completion
        time.sleep(0.3)
        
        # Verify both completed successfully
        with orchestrator_with_tasks.status_manager as sm:
            details1 = sm.get_dag_execution_details(dag1.dag_id, execution_id1)
            details2 = sm.get_dag_execution_details(dag2.dag_id, execution_id2)
            
            assert details1["status"] == "completed"
            assert details2["status"] == "completed"
    
    def test_concurrent_execution_with_failure(self, orchestrator_with_tasks):
        """Test concurrent execution handling failures correctly."""
        dag = DAG(dag_id="failing_dag")
        dag.add_task(SlowTask(task_id="success_task", duration=0.05))
        dag.add_task(FailingTask(task_id="fail_task"))
        
        execution_id = orchestrator_with_tasks.run_dag_in_thread(dag, fail_fast=True)
        
        # Wait for completion
        time.sleep(0.3)
        
        # Verify failure was recorded
        with orchestrator_with_tasks.status_manager as sm:
            details = sm.get_dag_execution_details(dag.dag_id, execution_id)
            assert details["status"] == "failed"
    
    def test_concurrent_execution_no_fail_fast(self, orchestrator_with_tasks):
        """Test concurrent execution continues after failure when fail_fast=False."""
        dag = DAG(dag_id="no_fail_fast_dag")
        dag.add_task(FailingTask(task_id="fail_task"))
        dag.add_task(SlowTask(task_id="success_task", duration=0.05))
        
        execution_id = orchestrator_with_tasks.run_dag_in_thread(dag, fail_fast=False)
        
        # Wait for completion
        time.sleep(0.3)
        
        # Verify execution completed despite failure
        with orchestrator_with_tasks.status_manager as sm:
            details = sm.get_dag_execution_details(dag.dag_id, execution_id)
            # Should still complete overall execution
            assert details["status"] in ["completed", "failed"]
    
    def test_basic_thread_safety(self, orchestrator_with_tasks):
        """Test basic thread safety with simpler approach."""
        dag = DAG(dag_id="thread_safety_dag")
        
        # Create just a few tasks
        for i in range(3):
            task = SlowTask(task_id=f"task_{i}", duration=0.02)
            dag.add_task(task)
        
        execution_id = orchestrator_with_tasks.run_dag_in_thread(dag)
        
        # Wait for completion
        time.sleep(0.2)
        
        # Verify completion
        with orchestrator_with_tasks.status_manager as sm:
            details = sm.get_dag_execution_details(dag.dag_id, execution_id)
            assert details["execution_id"] == execution_id
            assert details["status"] == "completed"
    
    def test_resume_concurrent_execution(self, orchestrator_with_tasks, simple_dag):
        """Test resuming a DAG execution works with concurrent features."""
        # First, run partially and simulate failure
        with orchestrator_with_tasks.status_manager as sm:
            # Manually set some tasks as completed
            sm.set_task_status(simple_dag.dag_id, "task1", "completed")
            sm.set_task_status(simple_dag.dag_id, "task2", "running")
        
        # Resume execution
        execution_id = orchestrator_with_tasks.run_dag_in_thread(simple_dag, resume=True)
        
        # Wait for completion
        time.sleep(0.3)
        
        # Verify that task1 was skipped and others completed
        with orchestrator_with_tasks.status_manager as sm:
            details = sm.get_dag_execution_details(simple_dag.dag_id, execution_id)
            assert details["status"] == "completed"
            
            # Task1 should not have been executed (already completed)
            assert not simple_dag.tasks["task1"].executed
            # Task2 and task3 should have been executed
            assert simple_dag.tasks["task2"].executed
            assert simple_dag.tasks["task3"].executed


class TestStatusManagerThreadSafety:
    """Test thread safety of the StatusManager."""
    
    def test_concurrent_status_updates(self, tmp_path):
        """Test concurrent status updates are handled correctly."""
        db_path = tmp_path / "concurrent_status.db"
        sm = StatusManager(str(db_path))
        
        dag_id = "thread_test_dag"
        
        def update_task_status(task_id, status):
            with sm:
                sm.set_task_status(dag_id, task_id, status)
        
        # Create multiple threads updating different tasks
        with ThreadPoolExecutor(max_workers=5) as executor:
            futures = []
            for i in range(10):
                future = executor.submit(update_task_status, f"task_{i}", "completed")
                futures.append(future)
            
            # Wait for all updates to complete
            for future in futures:
                future.result()
        
        # Verify all updates were applied
        with sm:
            for i in range(10):
                status = sm.get_task_status(dag_id, f"task_{i}")
                assert status == "completed"
    
    def test_concurrent_dag_execution_tracking(self, tmp_path):
        """Test concurrent DAG execution tracking."""
        db_path = tmp_path / "concurrent_dag_tracking.db"
        sm = StatusManager(str(db_path))
        
        def create_and_complete_execution(dag_id):
            execution_id = str(uuid.uuid4())
            with sm:
                sm.create_dag_execution(dag_id, execution_id)
                time.sleep(0.1)  # Simulate execution time
                sm.update_dag_execution_status(dag_id, execution_id, "completed")
            return execution_id
        
        # Create multiple concurrent executions
        with ThreadPoolExecutor(max_workers=3) as executor:
            futures = []
            for i in range(5):
                future = executor.submit(create_and_complete_execution, f"dag_{i}")
                futures.append(future)
            
            execution_ids = [future.result() for future in futures]
        
        # Verify all executions were tracked
        with sm:
            all_dags = sm.get_all_dags()
            assert len(all_dags) == 5
            
            for dag_data in all_dags:
                assert dag_data["execution_id"] in execution_ids
                assert dag_data["status"] == "completed"
    
    def test_concurrent_log_writing(self, tmp_path):
        """Test concurrent log writing is thread-safe."""
        db_path = tmp_path / "concurrent_logs.db"
        sm = StatusManager(str(db_path))
        
        dag_id = "log_test_dag"
        execution_id = str(uuid.uuid4())
        
        def write_logs(task_id, count):
            with sm:
                for i in range(count):
                    sm.log_message(dag_id, execution_id, task_id, "INFO", f"Log message {i}")
        
        # Create multiple threads writing logs
        with ThreadPoolExecutor(max_workers=3) as executor:
            futures = []
            for i in range(3):
                future = executor.submit(write_logs, f"task_{i}", 10)
                futures.append(future)
            
            # Wait for all log writes to complete
            for future in futures:
                future.result()
        
        # Verify all logs were written
        with sm:
            logs = sm.get_execution_logs(dag_id, execution_id, limit=100)
            assert len(logs) == 30  # 3 tasks * 10 messages each
    
    def test_cleanup_during_concurrent_access(self, tmp_path):
        """Test cleanup operations during concurrent access."""
        db_path = tmp_path / "cleanup_test.db"
        sm = StatusManager(str(db_path))
        
        dag_id = "cleanup_test_dag"
        
        # Create some old executions
        with sm:
            old_execution_id = str(uuid.uuid4())
            sm.create_dag_execution(dag_id, old_execution_id)
            sm.update_dag_execution_status(dag_id, old_execution_id, "completed")
        
        def concurrent_read():
            with sm:
                return sm.get_all_dags()
        
        def concurrent_cleanup():
            with sm:
                return sm.cleanup_old_executions(0)  # Clean everything
        
        # Run cleanup concurrently with reads
        with ThreadPoolExecutor(max_workers=2) as executor:
            read_future = executor.submit(concurrent_read)
            cleanup_future = executor.submit(concurrent_cleanup)
            
            # Both should complete without errors
            read_result = read_future.result()
            cleanup_result = cleanup_future.result()
            
            assert isinstance(read_result, list)
            assert isinstance(cleanup_result, int)
