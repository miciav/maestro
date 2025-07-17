import pytest
import os

from maestro.core.orchestrator import Orchestrator
from maestro.tasks.base import BaseTask
from maestro.core.task import TaskStatus
from maestro.core.status_manager import StatusManager

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
    orchestrator.run_dag(dag)

    with StatusManager(db_path) as sm:
        assert sm.get_task_status(dag.dag_id, "task1") == "completed"
        assert sm.get_task_status(dag.dag_id, "task2") == "completed"

def test_resume_execution(orchestrator, dag_filepath, db_path):
    dag = orchestrator.load_dag_from_file(dag_filepath)

    with StatusManager(db_path) as sm:
        sm.set_task_status(dag.dag_id, "task1", "completed")

    assert not dag.tasks["task1"].executed
    assert not dag.tasks["task2"].executed

    orchestrator.run_dag(dag, resume=True)

    assert not dag.tasks["task1"].executed
    assert dag.tasks["task2"].executed
    assert dag.tasks["task1"].status == TaskStatus.COMPLETED
    assert dag.tasks["task2"].status == TaskStatus.COMPLETED

def test_reset_execution(orchestrator, dag_filepath, db_path):
    dag1 = orchestrator.load_dag_from_file(dag_filepath)
    orchestrator.run_dag(dag1)
    assert dag1.tasks["task1"].executed

    dag2 = orchestrator.load_dag_from_file(dag_filepath)
    assert not dag2.tasks["task1"].executed

    orchestrator.run_dag(dag2, resume=False)

    assert dag2.tasks["task1"].executed
    with StatusManager(db_path) as sm:
        assert sm.get_task_status(dag1.dag_id, "task1") == "completed"

