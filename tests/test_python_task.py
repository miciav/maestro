# tests/test_python_task.py

from unittest.mock import MagicMock, patch

import pytest

from maestro.server.tasks.python_task import PythonTask


# FIXTURE: finto StatusManager
@pytest.fixture
def fake_status_manager():
    """
    Restituisce un finto StatusManager con metodi
    add_log() e set_task_output() tracciabili.
    """
    fake = MagicMock()
    fake.add_log = MagicMock()
    fake.set_task_output = MagicMock()
    return fake


@pytest.fixture
def patched_status_manager(fake_status_manager):
    with patch(
        "maestro.server.tasks.python_task.StatusManager.get_instance",
        return_value=fake_status_manager,
    ):
        yield fake_status_manager


# TEST 1 — Cattura del print (logging INFO)
def test_python_task_captures_print(patched_status_manager):
    task = PythonTask(
        task_id="t1", dag_id="dagX", execution_id="exec1", code="print('ciao')"
    )

    task.execute_local()

    patched_status_manager.add_log.assert_any_call(
        dag_id="dagX",
        execution_id="exec1",
        task_id="t1",
        level="INFO",
        message="[PythonTask] ciao",
    )


# TEST 2 — Esecuzione inline + cattura output di print()
def test_python_task_captures_task_output(patched_status_manager):
    task = PythonTask(
        task_id="t1", dag_id="dagX", execution_id="exec1", code="task_output = 123"
    )

    task.execute_local()

    patched_status_manager.set_task_output.assert_called_once_with(
        dag_id="dagX", task_id="t1", execution_id="exec1", output=123
    )


# TEST 3 — Esecuzione da file


def test_python_task_executes_script_file(patched_status_manager, tmp_path):
    script_path = tmp_path / "myscript.py"
    script_path.write_text("task_output = 99")

    task = PythonTask(
        task_id="t1", dag_id="dagX", execution_id="exec1", script_path=str(script_path)
    )

    task.execute_local()

    patched_status_manager.set_task_output.assert_called_once_with(
        dag_id="dagX", task_id="t1", execution_id="exec1", output=99
    )


# TEST 4 — Priorità dell’output


# 4.1 Caso task_output
def test_python_task_priority_task_output(patched_status_manager):
    task = PythonTask(
        task_id="t1",
        dag_id="d",
        execution_id="e",
        code="task_output = 'A'; __output__='B'; result='C'; output='D'",
    )
    task.execute_local()

    patched_status_manager.set_task_output.assert_called_once_with(
        dag_id="d", task_id="t1", execution_id="e", output="A"
    )


# 4.2 Caso __output__
def test_python_task_priority_dunder_output(patched_status_manager):
    task = PythonTask(
        task_id="t1",
        dag_id="d",
        execution_id="e",
        code="__output__='B'; result='C'; output='D'",
    )
    task.execute_local()

    patched_status_manager.set_task_output.assert_called_once_with(
        dag_id="d", task_id="t1", execution_id="e", output="B"
    )


# 4.3 Caso result
def test_python_task_priority_result(patched_status_manager):
    task = PythonTask(
        task_id="t1", dag_id="d", execution_id="e", code="result='C'; output='D'"
    )
    task.execute_local()

    patched_status_manager.set_task_output.assert_called_once_with(
        dag_id="d", task_id="t1", execution_id="e", output="C"
    )


# 4.4 Caso output
def test_python_task_priority_output(patched_status_manager):
    task = PythonTask(task_id="t1", dag_id="d", execution_id="e", code="output='D'")
    task.execute_local()

    patched_status_manager.set_task_output.assert_called_once_with(
        dag_id="d", task_id="t1", execution_id="e", output="D"
    )


# TEST 5 — Gestione eccezioni
def test_python_task_logs_and_raises_exceptions(patched_status_manager):
    task = PythonTask(
        task_id="t1", dag_id="d", execution_id="e", code="raise ValueError('BOOM')"
    )

    with pytest.raises(ValueError):
        task.execute_local()

    # Controllo che abbia loggato almeno un errore
    error_logs = [
        call
        for call in patched_status_manager.add_log.call_args_list
        if call.kwargs.get("level") == "ERROR"
    ]

    assert len(error_logs) > 0
