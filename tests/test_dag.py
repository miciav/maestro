
import pytest

from maestro.core.dag import DAG
from maestro.core.task import Task

class DummyTask(Task):
    def execute_local(self):
        pass

def test_dag_add_task():
    dag = DAG()
    task = DummyTask(task_id="test_task")
    dag.add_task(task)
    assert "test_task" in dag.tasks

def test_dag_validation():
    dag = DAG()
    task1 = DummyTask(task_id="task1")
    task2 = DummyTask(task_id="task2", dependencies=["task1"])
    dag.add_task(task1)
    dag.add_task(task2)
    dag.validate()

def test_dag_cycle_detection():
    dag = DAG()
    task1 = DummyTask(task_id="task1", dependencies=["task3"])
    task2 = DummyTask(task_id="task2", dependencies=["task1"])
    task3 = DummyTask(task_id="task3", dependencies=["task2"])
    dag.add_task(task1)
    dag.add_task(task2)
    dag.add_task(task3)
    with pytest.raises(ValueError, match="DAG has a cycle."):
        dag.validate()
