import pytest
import os
from unittest.mock import MagicMock

from maestro.core.orchestrator import Orchestrator
from maestro.core.dag import DAG
from maestro.tasks.base import BaseTask
from maestro.core.task import TaskStatus

# Define a dummy task for testing
class DummyPrintTask(BaseTask):
    message: str
    executed: bool = False

    def execute(self):
        self.executed = True
        print(f"DummyPrintTask {self.task_id}: {self.message}")


@pytest.fixture
def orchestrator():
    orch = Orchestrator(log_level="CRITICAL") # Suppress logging during tests
    orch.register_task_type("print_task", DummyPrintTask)
    return orch

@pytest.fixture
def dag_filepath(tmp_path):
    content = """
dag:
  tasks:
    - task_id: task1
      type: print_task
      message: "Hello from task1"
    - task_id: task2
      type: print_task
      message: "Hello from task2"
      dependencies: [task1]
"""
    f = tmp_path / "test_dag.yaml"
    f.write_text(content)
    return str(f)

@pytest.fixture
def invalid_dag_filepath(tmp_path):
    content = """
dag:
  tasks:
    - task_id: task1
      type: non_existent_task
      message: "Hello from task1"
"""
    f = tmp_path / "invalid_dag.yaml"
    f.write_text(content)
    return str(f)

def test_orchestrator_load_dag_from_file(orchestrator, dag_filepath):
    dag = orchestrator.load_dag_from_file(dag_filepath)
    assert isinstance(dag, DAG)
    assert "task1" in dag.tasks
    assert "task2" in dag.tasks
    assert dag.tasks["task2"].dependencies == ["task1"]

def test_orchestrator_load_invalid_dag(orchestrator, invalid_dag_filepath):
    with pytest.raises(ValueError, match="Unknown task type: non_existent_task"):
        orchestrator.load_dag_from_file(invalid_dag_filepath)

def test_orchestrator_run_dag(orchestrator, dag_filepath):
    dag = orchestrator.load_dag_from_file(dag_filepath)
    
    # Mock status manager and progress tracker for testing
    mock_status_manager = MagicMock()
    mock_progress_tracker = MagicMock()
    mock_status_callback = MagicMock()

    orchestrator.run_dag(dag, 
                          status_manager=mock_status_manager, 
                          progress_tracker=mock_progress_tracker,
                          status_callback=mock_status_callback)

    assert dag.tasks["task1"].status == TaskStatus.COMPLETED
    assert dag.tasks["task2"].status == TaskStatus.COMPLETED
    assert dag.tasks["task1"].executed # Check if execute was called
    assert dag.tasks["task2"].executed # Check if execute was called

    # Verify status manager calls
    mock_status_manager.set_task_status.assert_any_call("task1", "running")
    mock_status_manager.set_task_status.assert_any_call("task1", "completed")
    mock_status_manager.set_task_status.assert_any_call("task2", "running")
    mock_status_manager.set_task_status.assert_any_call("task2", "completed")

    # Verify progress tracker calls
    mock_progress_tracker.increment_completed.assert_called()

    # Verify status callback calls
    assert mock_status_callback.call_count >= 4 # At least 2 for running, 2 for completed

def test_orchestrator_run_dag_fail_fast(orchestrator, tmp_path):
    # Create a DAG with a failing task
    content = """
dag:
  tasks:
    - task_id: failing_task
      type: failing_task_type
    - task_id: subsequent_task
      type: print_task
      message: "This should not run"
      dependencies: [failing_task]
"""
    f = tmp_path / "failing_dag.yaml"
    f.write_text(content)

    class FailingTask(BaseTask):
        def execute(self):
            raise Exception("Simulated task failure")

    orchestrator.register_task_type("failing_task_type", FailingTask)

    dag = orchestrator.load_dag_from_file(str(f))

    with pytest.raises(Exception, match="Task failing_task failed: Simulated task failure"):
        orchestrator.run_dag(dag, fail_fast=True)

    assert dag.tasks["failing_task"].status == TaskStatus.FAILED
    assert dag.tasks["subsequent_task"].status == TaskStatus.PENDING # Should not have run

def test_orchestrator_run_dag_no_fail_fast(orchestrator, tmp_path):
    # Create a DAG with a failing task
    content = """
dag:
  tasks:
    - task_id: failing_task
      type: failing_task_type
    - task_id: subsequent_task
      type: print_task
      message: "This should run"
      dependencies: [failing_task]
"""
    f = tmp_path / "no_fail_fast_dag.yaml"
    f.write_text(content)

    class FailingTask(BaseTask):
        def execute(self):
            raise Exception("Simulated task failure")

    orchestrator.register_task_type("failing_task_type", FailingTask)

    dag = orchestrator.load_dag_from_file(str(f))

    orchestrator.run_dag(dag, fail_fast=False)

    assert dag.tasks["failing_task"].status == TaskStatus.FAILED
    assert dag.tasks["subsequent_task"].status == TaskStatus.COMPLETED # Should have run

