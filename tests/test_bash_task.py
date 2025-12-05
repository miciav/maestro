from unittest.mock import Mock, patch

import pytest

from maestro.server.internals.status_manager import StatusManager
from maestro.server.tasks.bash_task import BashTask


@pytest.fixture
def mock_status_manager():
    sm = Mock()
    sm.add_log = Mock()
    sm.set_task_output = Mock()
    # Patch get_instance() to return our mock
    with patch.object(StatusManager, "get_instance", return_value=sm):
        yield sm


@pytest.fixture
def mock_popen():
    """Creates a fake Popen object we can control."""
    mock_process = Mock()

    # Default: no output, success
    mock_process.stdout = []
    mock_process.stderr = []
    mock_process.returncode = 0

    mock_process.wait = Mock()
    return mock_process


def test_bash_task_logs_stdout(mock_status_manager, mock_popen):
    mock_popen.stdout = ["hello\n", "world\n"]

    with patch("subprocess.Popen", return_value=mock_popen):
        task = BashTask(task_id="t1", command="echo test")
        task.dag_id = "D1"
        task.execution_id = "E1"

        task.execute_local()

    # Check logs
    assert mock_status_manager.add_log.call_count == 2
    mock_status_manager.add_log.assert_any_call(
        dag_id="D1",
        execution_id="E1",
        task_id="t1",
        message="[BashTask][stdout] hello",
        level="INFO",
    )


def test_bash_task_logs_stderr(mock_status_manager, mock_popen):
    mock_popen.stderr = ["error line\n"]

    with patch("subprocess.Popen", return_value=mock_popen):
        task = BashTask(task_id="t1", command="bad")
        task.dag_id = "D1"
        task.execution_id = "E1"

        task.execute_local()

    mock_status_manager.add_log.assert_any_call(
        dag_id="D1",
        execution_id="E1",
        task_id="t1",
        message="[BashTask][stderr] error line",
        level="ERROR",
    )


def test_bash_task_captures_output_line(mock_status_manager, mock_popen):
    mock_popen.stdout = ["OUTPUT: 42\n"]

    with patch("subprocess.Popen", return_value=mock_popen):
        task = BashTask(task_id="t1", command="echo OUTPUT:")
        task.dag_id = "D1"
        task.execution_id = "E1"

        task.execute_local()

    mock_status_manager.set_task_output.assert_called_once_with("D1", "t1", "E1", "42")


def test_bash_task_fails_on_nonzero_exit_code(mock_status_manager, mock_popen):
    mock_popen.returncode = 3

    with patch("subprocess.Popen", return_value=mock_popen):
        task = BashTask(task_id="t1", command="exit 3")
        task.dag_id = "D1"
        task.execution_id = "E1"

        with pytest.raises(Exception, match="exit code 3"):
            task.execute_local()
