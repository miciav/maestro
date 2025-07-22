import pytest
import os
import uuid

from maestro.server.internals.orchestrator import Orchestrator
from maestro.tasks.base import BaseTask
from maestro.shared.task import TaskStatus
from maestro.server.internals.status_manager import StatusManager

# Define a dummy task for testing
class DummyPrintTask(BaseTask):
    message: str
    executed: bool = False

    def execute_local(self):
        self.executed = True

@pytest.fixture
def db_path(tmp_path):
    return os.path.join(tmp_path, "test_maestro.db")

@pytest.fixture
def orchestrator(db_path):
    orch = Orchestrator(log_level="CRITICAL", db_path=db_path)
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

def test_db_creation(orchestrator, db_path):
    with orchestrator.status_manager:
        pass  # Entering the context creates the DB
    assert os.path.exists(db_path)

def test_save_state(orchestrator, dag_filepath, db_path):
    dag = orchestrator.load_dag_from_file(dag_filepath)
    execution_id = orchestrator.run_dag_in_thread(dag)
    
    # Wait for execution to complete
    import time
    time.sleep(2)

    with StatusManager(db_path) as sm:
        assert sm.get_task_status(dag.dag_id, "task1", execution_id) == "completed"
        assert sm.get_task_status(dag.dag_id, "task2", execution_id) == "completed"

def test_resume_execution(orchestrator, dag_filepath, db_path):
    dag = orchestrator.load_dag_from_file(dag_filepath)
    
    # First, create an execution and set task1 as completed
    execution_id = str(uuid.uuid4())
    with StatusManager(db_path) as sm:
        # Create the execution and DAG records
        sm.save_dag_definition(dag)
        sm.create_dag_execution(dag.dag_id, execution_id)
        sm.initialize_tasks_for_execution(dag.dag_id, execution_id, list(dag.tasks.keys()))
        # Mark task1 as completed
        sm.set_task_status(dag.dag_id, "task1", "completed", execution_id)

    assert not dag.tasks["task1"].executed
    assert not dag.tasks["task2"].executed

    # Run the DAG with resume=True using the same execution_id
    orchestrator.run_dag(dag, execution_id=execution_id, resume=True)

    assert not dag.tasks["task1"].executed  # Should not re-execute
    assert dag.tasks["task2"].executed      # Should execute
    assert dag.tasks["task1"].status == TaskStatus.COMPLETED
    assert dag.tasks["task2"].status == TaskStatus.COMPLETED

def test_reset_execution(orchestrator, dag_filepath, db_path):
    dag1 = orchestrator.load_dag_from_file(dag_filepath)
    execution_id1 = orchestrator.run_dag_in_thread(dag1)
    
    # Wait for execution to complete
    import time
    time.sleep(2)
    
    assert dag1.tasks["task1"].executed

    dag2 = orchestrator.load_dag_from_file(dag_filepath)
    assert not dag2.tasks["task1"].executed  # New DAG instance has fresh tasks

    execution_id2 = orchestrator.run_dag_in_thread(dag2, resume=False)
    time.sleep(2)

    assert dag2.tasks["task1"].executed
    with StatusManager(db_path) as sm:
        # Both executions should show completed
        assert sm.get_task_status(dag1.dag_id, "task1", execution_id1) == "completed"
        assert sm.get_task_status(dag2.dag_id, "task1", execution_id2) == "completed"

