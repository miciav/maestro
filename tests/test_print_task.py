from unittest.mock import MagicMock, patch

import pytest

from maestro.server.internals.status_manager import StatusManager
from maestro.server.tasks.print_task import PrintTask


# --- FIXTURE per StatusManager mockato
@pytest.fixture
def mock_status_manager():
    mock = MagicMock()
    with patch.object(StatusManager, "get_instance", return_value=mock):
        yield mock


# --- TEST 1: La PrintTask stampa correttamente
def test_print_task_prints_message(mock_status_manager):
    task = PrintTask(
        task_id="T1", dag_id="DAGX", execution_id="EX1", message="Hello World"
    )

    with patch("builtins.print") as mock_print:
        task.execute_local()
        mock_print.assert_called_with("[PrintTask] Hello World", flush=True)


# --- TEST 2: Viene chiamato add_log con i parametri corretti
def test_print_task_adds_log_to_status_manager(mock_status_manager):
    task = PrintTask(task_id="T1", dag_id="DAGX", execution_id="EX1", message="ABC")

    with patch("builtins.print"):
        task.execute_local()

    mock_status_manager.add_log.assert_called_once_with(
        dag_id="DAGX",
        execution_id="EX1",
        task_id="T1",
        message="[PrintTask] ABC",
        level="INFO",
    )


# --- TEST 3: message vuoto
def test_print_task_empty_message(mock_status_manager):
    task = PrintTask(task_id="T1", dag_id="DAGX", execution_id="EX1", message="")

    with patch("builtins.print") as mock_print:
        task.execute_local()
        mock_print.assert_called_with("[PrintTask] ", flush=True)
