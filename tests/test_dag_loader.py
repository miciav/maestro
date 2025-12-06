from typing import Any, Optional
from unittest.mock import Mock, patch

import pytest
from pydantic import Field

from maestro.server.internals.dag_loader import DAGLoader
from maestro.server.internals.task_registry import TaskRegistry
from maestro.server.tasks.base import BaseTask
from maestro.shared.dag import DAG


# ==============================
# DummyTask globale per tutti i test
# ==============================
class DummyTask(BaseTask):
    """
    DummyTask per i test:
    - eredita correttamente da BaseTask (che è un Pydantic model)
    - permette campi extra (foo, message, ecc.)
    - dichiara foo e message come opzionali per eliminare warning VSCode
    """

    foo: Optional[Any] = Field(default=None)
    message: Optional[str] = Field(default=None)

    class Config:
        extra = "allow"  # <-- LA PARTE FONDAMENTALE

    def execute_local(self):
        pass


# -----------------------------
# Fixtures
# -----------------------------
@pytest.fixture
def mock_task_registry():
    registry = Mock(spec=TaskRegistry)
    # Ora registry.get() restituisce sempre DummyTask
    registry.get.return_value = DummyTask
    return registry


@pytest.fixture
def dag_loader(mock_task_registry):
    return DAGLoader(task_registry=mock_task_registry)


# -----------------------------
# Test: load DAG from dictionary
# -----------------------------
def test_load_dag_from_dict_minimal(dag_loader):
    dag_dict = {"dag_id": "dag1", "tasks": {"task1": {"type": "DummyTask"}}}
    dag = dag_loader.load_dag_from_dict(dag_dict)

    assert isinstance(dag, DAG)
    assert len(dag.tasks) == 1

    # Accediamo al task corretto dal dict
    task = dag.tasks["task1"]  # task_id è la chiave del dict
    assert task.task_id == "task1"


def test_load_dag_from_dict_infers_printtask(dag_loader, mock_task_registry):
    # Registriamo DummyTask come PrintTask fittizio
    mock_task_registry.register("PrintTask", DummyTask)
    dag_loader.task_registry = mock_task_registry

    dag_dict = {"dag_id": "dag2", "tasks": {"t1": {"message": "hello world"}}}

    dag = dag_loader.load_dag_from_dict(dag_dict)

    # Accediamo al task
    task = dag.tasks["t1"]
    assert task.task_id == "t1"
    assert hasattr(task, "message")
    assert task.message == "hello world"


def test_load_dag_from_dict_invalid_type_raises(dag_loader, mock_task_registry):
    dag_dict = {"dag_id": "dag3", "tasks": {"t1": {"type": "UnknownTask"}}}
    # Make registry return None
    mock_task_registry.get.return_value = None
    with pytest.raises(ValueError, match="Unknown task type"):
        dag_loader.load_dag_from_dict(dag_dict)


# -----------------------------
# Test: datetime parsing
# -----------------------------
@pytest.mark.parametrize(
    "dt_str,expected",
    [
        ("2023-12-25 14:30:00", (2023, 12, 25, 14, 30, 0)),
        ("2023-12-25", (2023, 12, 25, 0, 0, 0)),
        ("2023-12-25T14:30", (2023, 12, 25, 14, 30, 0)),
    ],
)
def test_parse_datetime_formats(dag_loader, dt_str, expected):
    dt = dag_loader._parse_datetime(dt_str)
    assert (dt.year, dt.month, dt.day, dt.hour, dt.minute, dt.second) == expected


def test_parse_datetime_invalid_raises(dag_loader):
    with pytest.raises(ValueError, match="Unable to parse datetime string"):
        dag_loader._parse_datetime("invalid-date")


# -----------------------------
# Test: create task from config merges params and preserves condition
# -----------------------------
def test_create_task_from_config_merges_params_and_condition(
    dag_loader, mock_task_registry
):
    # Registriamo DummyTask nel registry per il test
    mock_task_registry.register("DummyTask", DummyTask)
    dag_loader.task_registry = mock_task_registry

    task_config = {
        "type": "DummyTask",
        "task_id": "t1",
        "params": {"foo": 42},
        "condition": "some_condition",
    }
    task = dag_loader._create_task_from_config(task_config, "dummy.yaml")

    # Verifica attributi
    assert task.task_id == "t1"
    assert hasattr(task, "foo")
    assert task.foo == 42
    assert hasattr(task, "condition")
    assert task.condition == "some_condition"


# -----------------------------
# Test: load DAG from file raises on missing file
# -----------------------------
def test_load_dag_from_file_missing_file(dag_loader):
    with pytest.raises(FileNotFoundError):
        dag_loader.load_dag_from_file("/non/existent/file.yaml")


# -----------------------------
# Test: load DAG from file raises on invalid YAML
# -----------------------------
def test_load_dag_from_file_invalid_yaml(dag_loader):
    # Patch open to return invalid YAML
    with patch("builtins.open", mock_open(read_data=":::")):
        with pytest.raises(ValueError, match="Invalid YAML configuration"):
            dag_loader.load_dag_from_file("dummy.yaml")


# Utility for mocking open
from io import StringIO
from unittest.mock import mock_open, patch


# -----------------------------
# Test: load DAG from file with valid YAML
# -----------------------------
def test_load_dag_from_file_valid_yaml(dag_loader, mock_task_registry):
    yaml_content = """
dag:
  dag_id: my_dag
  tasks:
    - type: DummyTask
      task_id: task1
"""
    m = mock_open(read_data=yaml_content)
    with patch("builtins.open", m):
        dag = dag_loader.load_dag_from_file("dummy.yaml")

    assert isinstance(dag, DAG)
    assert len(dag.tasks) == 1

    # Accediamo correttamente al task
    task = list(dag.tasks.values())[0]  # oppure dag.tasks["task1"] se chiave nota
    assert task.task_id == "task1"


# -----------------------------
# Test: load DAG from file with both start_time and cron_schedule -> ValueError
# -----------------------------
def test_load_dag_from_file_both_start_and_cron(dag_loader):
    yaml_content = """
dag:
  dag_id: dag1
  start_time: "2023-12-25 14:30:00"
  cron_schedule: "* * * * *"
  tasks:
    - type: DummyTask
      task_id: t1
"""
    m = mock_open(read_data=yaml_content)
    with patch("builtins.open", m):
        with pytest.raises(
            ValueError, match="Cannot specify both start_time and cron_schedule"
        ):
            dag_loader.load_dag_from_file("dummy.yaml")


# -----------------------------
# Test: load DAG from file with invalid start_time format -> ValueError
# -----------------------------
def test_load_dag_from_file_invalid_start_time(dag_loader):
    yaml_content = """
dag:
  dag_id: dag1
  start_time: "not-a-date"
  tasks:
    - type: DummyTask
      task_id: t1
"""
    m = mock_open(read_data=yaml_content)
    with patch("builtins.open", m):
        with pytest.raises(ValueError, match="Invalid start_time format"):
            dag_loader.load_dag_from_file("dummy.yaml")


# -----------------------------
# Test: load DAG from file missing 'tasks' field -> ValueError
# -----------------------------
def test_load_dag_from_file_missing_tasks(dag_loader):
    yaml_content = """
dag:
  dag_id: dag1
"""
    m = mock_open(read_data=yaml_content)
    with patch("builtins.open", m):
        with pytest.raises(
            ValueError, match="DAG configuration must contain 'tasks' field"
        ):
            dag_loader.load_dag_from_file("dummy.yaml")


# -----------------------------
# Test integrato DAGLoader con più task
# -----------------------------
def test_load_dag_from_dict_multiple_tasks(dag_loader, mock_task_registry):
    # Registriamo DummyTask
    mock_task_registry.register("DummyTask", DummyTask)
    dag_loader.task_registry = mock_task_registry

    dag_dict = {
        "dag_id": "integration_dag",
        "tasks": {
            "task1": {"type": "DummyTask", "task_id": "task1"},
            "task2": {"type": "DummyTask", "task_id": "task2"},
            "task3": {"type": "DummyTask", "task_id": "task3"},
        },
    }

    dag = dag_loader.load_dag_from_dict(dag_dict)

    # Verifica DAG
    assert isinstance(dag, DAG)
    assert dag.dag_id == "integration_dag"
    assert len(dag.tasks) == 3

    # Ora accediamo ai task correttamente (dag.tasks è dict)
    task_ids = {task.task_id for task in dag.tasks.values()}
    assert task_ids == {"task1", "task2", "task3"}
