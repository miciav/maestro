import pytest

from maestro.server.tasks.base import BaseTask


def test_basetask_initialization_defaults():
    task = BaseTask(task_id="t1")

    assert task.retries == 0
    assert task.retry_delay == 0
    assert task.dag_id is None
    assert task.execution_id is None


def test_basetask_custom_fields():
    task = BaseTask(
        task_id="t1", retries=3, retry_delay=10, dag_id="D1", execution_id="E1"
    )

    assert task.retries == 3
    assert task.retry_delay == 10
    assert task.dag_id == "D1"
    assert task.execution_id == "E1"


def test_basetask_execute_local_not_implemented():
    task = BaseTask(task_id="t1")

    with pytest.raises(NotImplementedError):
        task.execute_local()
