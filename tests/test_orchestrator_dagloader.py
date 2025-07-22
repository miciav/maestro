import pytest
from datetime import datetime

from maestro.server.internals.orchestrator import Orchestrator
from maestro.shared.dag import DAG
from maestro.tasks.base import BaseTask
from maestro.shared.task import TaskStatus
from maestro.server.internals.executors.local import LocalExecutor

# Define a dummy task for testing
class DummyPrintTask(BaseTask):
    message: str
    executed: bool = False

    def execute_local(self):
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

@pytest.fixture
def dag_with_start_time_filepath(tmp_path):
    content = """
dag:
  name: "scheduled_dag"
  start_time: "2024-01-01T09:00:00"
  tasks:
    - task_id: task1
      type: print_task
      message: "Hello from scheduled task1"
    - task_id: task2
      type: print_task
      message: "Hello from scheduled task2"
      dependencies: [task1]
"""
    f = tmp_path / "scheduled_dag.yaml"
    f.write_text(content)
    return str(f)


@pytest.fixture
def dag_with_cron_schedule_filepath(tmp_path):
    content = """
dag:
  name: "cron_scheduled_dag"
  cron_schedule: "0 9 * * *"  # Every day at 9:00 AM
  tasks:
    - task_id: task1
      type: print_task
      message: "Hello from scheduled task1"
    - task_id: task2
      type: print_task
      message: "Hello from scheduled task2"
      dependencies: [task1]
"""
    f = tmp_path / "cron_scheduled_dag.yaml"
    f.write_text(content)
    return str(f)


@pytest.fixture
def dag_with_invalid_start_time_filepath(tmp_path):
    content = """
dag:
  name: "invalid_scheduled_dag"
  start_time: "invalid-date-format"
  tasks:
    - task_id: task1
      type: print_task
      message: "Hello from task1"
"""
    f = tmp_path / "invalid_scheduled_dag.yaml"
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
    
    orchestrator.run_dag(dag)

    assert dag.tasks["task1"].status == TaskStatus.COMPLETED
    assert dag.tasks["task2"].status == TaskStatus.COMPLETED
    assert dag.tasks["task1"].executed # Check if execute was called
    assert dag.tasks["task2"].executed # Check if execute was called

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
        def execute_local(self):
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
        def execute_local(self):
            raise Exception("Simulated task failure")

    orchestrator.register_task_type("failing_task_type", FailingTask)

    dag = orchestrator.load_dag_from_file(str(f))

    orchestrator.run_dag(dag, fail_fast=False)

    assert dag.tasks["failing_task"].status == TaskStatus.FAILED
    assert dag.tasks["subsequent_task"].status == TaskStatus.SKIPPED

    def test_orchestrator_run_dag_with_skipped_task_marks_dag_as_failed(orchestrator, tmp_path):
        # Create a DAG with a failing task, which should cause the next to be skipped
        content = """
    dag:
      tasks:
        - task_id: failing_task
          type: failing_task_type
        - task_id: subsequent_task
          type: print_task
          message: "This should be skipped"
          dependencies: [failing_task]
    """
        f = tmp_path / "dag_with_skipped.yaml"
        f.write_text(content)

        class FailingTask(BaseTask):
            def execute_local(self):
                raise Exception("Simulated task failure")

        orchestrator.register_task_type("failing_task_type", FailingTask)

        dag = orchestrator.load_dag_from_file(str(f))

        # Use run_dag_in_thread to get the final status
        execution_id = orchestrator.run_dag_in_thread(dag, fail_fast=False)

        # Wait for the DAG to finish
        time.sleep(1) # Adjust as needed

        with orchestrator.status_manager as sm:
            dag_execution_details = sm.get_dag_execution_details(dag.dag_id, execution_id)
            assert dag_execution_details["status"] == "failed"

def test_orchestrator_load_dag_with_start_time(orchestrator, dag_with_start_time_filepath):
    """Test loading a DAG with start_time parameter from YAML file."""
    dag = orchestrator.load_dag_from_file(dag_with_start_time_filepath)
    
    assert isinstance(dag, DAG)
    assert dag.start_time is not None
    assert dag.start_time == datetime(2024, 1, 1, 9, 0, 0)
    assert "task1" in dag.tasks
    assert "task2" in dag.tasks
    assert dag.tasks["task2"].dependencies == ["task1"]

def test_orchestrator_load_dag_with_invalid_start_time(orchestrator, dag_with_invalid_start_time_filepath):
    """Test loading a DAG with invalid start_time format from YAML file."""
    with pytest.raises(ValueError, match="Invalid start_time format"):
        orchestrator.load_dag_from_file(dag_with_invalid_start_time_filepath)

def test_orchestrator_load_dag_without_start_time(orchestrator, dag_filepath):
    """Test loading a DAG without start_time parameter from YAML file."""
    dag = orchestrator.load_dag_from_file(dag_filepath)
    
    assert isinstance(dag, DAG)
    assert dag.start_time is None
    assert dag.is_ready_to_start()  # Should be ready to start immediately
    assert dag.time_until_start() is None

def test_orchestrator_load_dag_with_cron_schedule(orchestrator, dag_with_cron_schedule_filepath):
    """Test loading a DAG with cron schedule from YAML file."""
    dag = orchestrator.load_dag_from_file(dag_with_cron_schedule_filepath)
    
    assert isinstance(dag, DAG)
    assert dag.cron_schedule == "0 9 * * *"
    assert dag.start_time is None
    assert "task1" in dag.tasks
    assert "task2" in dag.tasks
    assert dag.tasks["task2"].dependencies == ["task1"]
    
    # Test schedule description
    assert "Cron schedule" in dag.get_schedule_description()
    
    # Test next run time
    next_run = dag.get_next_run_time()
    assert next_run is not None
    assert next_run.hour == 9
    assert next_run.minute == 0

