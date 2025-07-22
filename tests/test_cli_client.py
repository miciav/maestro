#!/usr/bin/env python3
"""
Comprehensive test suite for the Maestro CLI client.

This test suite ensures high coverage of all CLI commands and edge cases,
using mocks to isolate the client from server dependencies.
"""

import pytest
from unittest.mock import patch, Mock, MagicMock
from typer.testing import CliRunner
from maestro.cli_client import app, check_server_connection
import tempfile
import os
from io import StringIO
import typer


class TestCliClient:
    """Test suite for CLI client commands."""

    def setup_method(self):
        """Set up test fixtures."""
        self.runner = CliRunner()
        self.mock_response_data = {
            'dag_id': 'test-dag-123',
            'execution_id': 'exec-456',
            'status': 'submitted',
            'submitted_at': '2025-07-18T18:33:35Z'
        }

    @pytest.fixture
    def mock_api_client(self, mocker):
        """Mock API client with all methods."""
        return mocker.patch('maestro.cli_client.api_client', autospec=True)

    @pytest.fixture
    def mock_check_server(self, mocker):
        """Mock server connection check."""
        return mocker.patch('maestro.cli_client.check_server_connection')

    @pytest.fixture
    def temp_dag_file(self):
        """Create a temporary DAG file for testing."""
        with tempfile.NamedTemporaryFile(mode='w', suffix='.yaml', delete=False) as f:
            f.write("dag_id: test-dag\ntasks: []")
            yield f.name
        os.unlink(f.name)

    

    # Status Command Tests
    @pytest.mark.unit
    def test_status_command_success(self, mock_api_client, mock_check_server):
        """Test successful status retrieval."""
        mock_api_client.get_dag_status.return_value = {
            'execution_id': 'exec-123',
            'status': 'running',
            'started_at': '2025-07-18T18:33:35Z',
            'completed_at': None,
            'thread_id': 'thread-1',
            'tasks': [{
                'task_id': 'task-1',
                'status': 'completed',
                'started_at': '2025-07-18T18:33:35Z',
                'completed_at': '2025-07-18T18:34:35Z'
            }]
        }

        result = self.runner.invoke(app, ['status', 'test-dag'])

        assert result.exit_code == 0
        assert "DAG Status: test-dag" in result.output
        assert "running" in result.output
        assert "task-1" in result.output

    @pytest.mark.unit
    def test_status_command_with_execution_id(self, mock_api_client, mock_check_server):
        """Test status retrieval with specific execution ID."""
        mock_api_client.get_dag_status.return_value = {
            'execution_id': 'exec-specific',
            'status': 'completed',
            'started_at': '2025-07-18T18:33:35Z',
            'completed_at': '2025-07-18T18:34:35Z',
            'thread_id': None,
            'tasks': []
        }

        result = self.runner.invoke(app, ['status', 'test-dag', '--execution-id', 'exec-specific'])

        assert result.exit_code == 0
        mock_api_client.get_dag_status.assert_called_once_with('test-dag', 'exec-specific')

    @pytest.mark.unit
    def test_status_command_not_found(self, mock_api_client, mock_check_server):
        """Test status retrieval for non-existent DAG."""
        mock_api_client.get_dag_status.side_effect = FileNotFoundError("DAG not found")

        result = self.runner.invoke(app, ['status', 'nonexistent-dag'])

        assert result.exit_code == 1
        assert "DAG execution not found" in result.output

    # Logs Command Tests
    @pytest.mark.unit
    def test_logs_command_success(self, mock_api_client, mock_check_server):
        """Test successful logs retrieval."""
        mock_api_client.get_dag_logs_v1.return_value = [
                {
                    'timestamp': '2025-07-18T18:33:35.123456Z',
                    'level': 'INFO',
                    'task_id': 'task-1',
                    'message': 'Task started'
                },
                {
                    'timestamp': '2025-07-18T18:33:36.123456Z',
                    'level': 'ERROR',
                    'task_id': 'task-2',
                    'message': 'Task failed'
                }
            ]

        result = self.runner.invoke(app, ['log', 'test-dag'])

        assert "Task started" in result.output
        assert "Task failed" in result.output

    @pytest.mark.unit
    def test_logs_command_with_filters(self, mock_api_client, mock_check_server):
        """Test logs retrieval with filters."""
        mock_api_client.get_dag_logs_v1.return_value = []

        result = self.runner.invoke(app, [
            'log', 'test-dag',
            '--limit', '50',
            '--task', 'specific-task',
            '--level', 'ERROR'
        ])

        assert result.exit_code == 0
        mock_api_client.get_dag_logs_v1.assert_called_once_with(
            'test-dag', None, 50, 'specific-task', 'ERROR'
        )

    @pytest.mark.unit
    def test_logs_command_no_logs(self, mock_api_client, mock_check_server):
        """Test logs retrieval when no logs exist."""
        mock_api_client.get_dag_logs_v1.return_value = []

        result = self.runner.invoke(app, ['log', 'test-dag'])

        assert result.exit_code == 0
        assert "No logs found for DAG: test-dag" in result.output

    # Running Command Tests
    @pytest.mark.unit
    def test_running_command_success(self, mock_api_client, mock_check_server):
        """Test successful running DAGs retrieval."""
        mock_api_client.list_dags_v1.return_value = [
                {
                    'dag_id': 'dag-1',
                    'execution_id': 'exec-1',
                    'status': 'running',
                    'started_at': '2025-07-18T18:33:35Z',
                    'completed_at': None,
                    'thread_id': 123
                },
                {
                    'dag_id': 'dag-2',
                    'execution_id': 'exec-2',
                    'status': 'running',
                    'started_at': '2025-07-18T18:34:35Z',
                    'completed_at': None,
                    'thread_id': 456
                }
            ]

        result = self.runner.invoke(app, ['ls', '--filter', 'active'])

        assert result.exit_code == 0
        assert "Maestro DAGs" in result.output
        assert "dag-1" in result.output
        assert "dag-2" in result.output
        assert "Total DAGs: 2" in result.output

    @pytest.mark.unit
    def test_running_command_no_running_dags(self, mock_api_client, mock_check_server):
        """Test running DAGs retrieval when none are running."""
        mock_api_client.list_dags_v1.return_value = []

        result = self.runner.invoke(app, ['ls', '--filter', 'active'])

        assert result.exit_code == 0
        assert "No DAGs found with filter 'active'" in result.output

    # Cancel Command Tests
    @pytest.mark.unit
    def test_cancel_command_success(self, mock_api_client, mock_check_server):
        """Test successful DAG cancellation."""
        mock_api_client.stop_dag.return_value = {
            'message': 'DAG cancelled successfully'
        }

        result = self.runner.invoke(app, ['stop', 'test-dag'])

        assert result.exit_code == 0
        assert "DAG cancelled successfully" in result.output

    @pytest.mark.unit
    def test_cancel_command_not_running(self, mock_api_client, mock_check_server):
        """Test cancellation of non-running DAG."""
        mock_api_client.stop_dag.return_value = {
            'message': 'DAG is not running'
        }

        result = self.runner.invoke(app, ['stop', 'test-dag'])

        assert result.exit_code == 0
        assert "DAG is not running" in result.output

    # Validate Command Tests
    @pytest.mark.unit
    def test_validate_command_success(self, mock_api_client, mock_check_server, temp_dag_file):
        """Test successful DAG validation."""
        mock_api_client.validate_dag.return_value = {
            'valid': True,
            'dag_id': 'test-dag',
            'tasks': [
                {
                    'task_id': 'task-1',
                    'type': 'python',
                    'dependencies': []
                },
                {
                    'task_id': 'task-2',
                    'type': 'shell',
                    'dependencies': ['task-1']
                }
            ],
            'total_tasks': 2
        }

        result = self.runner.invoke(app, ['validate', temp_dag_file])

        assert result.exit_code == 0
        assert "✓ DAG is valid" in result.output
        assert "test-dag" in result.output
        assert "task-1" in result.output
        assert "task-2" in result.output
        assert "Total tasks: 2" in result.output

    @pytest.mark.unit
    def test_validate_command_invalid(self, mock_api_client, mock_check_server, temp_dag_file):
        """Test DAG validation failure."""
        mock_api_client.validate_dag.return_value = {
            'valid': False,
            'error': 'Invalid DAG structure'
        }

        result = self.runner.invoke(app, ['validate', temp_dag_file])

        assert result.exit_code == 1
        assert "✗ DAG validation failed" in result.output
        assert "Invalid DAG structure" in result.output

    # Cleanup Command Tests
    @pytest.mark.unit
    def test_cleanup_command_success(self, mock_api_client, mock_check_server):
        """Test successful cleanup."""
        mock_api_client.cleanup_old_executions.return_value = {
            'message': 'Cleaned up 5 old executions'
        }

        result = self.runner.invoke(app, ['cleanup', '--days', '7'])

        assert result.exit_code == 0
        assert "Cleaned up 5 old executions" in result.output
        mock_api_client.cleanup_old_executions.assert_called_once_with(7)

    # List Command Tests
    @pytest.mark.unit
    def test_list_command_success(self, mock_api_client, mock_check_server):
        """Test successful DAG listing."""
        mock_api_client.list_dags_v1.return_value = [
                {
                    'dag_id': 'dag-1',
                    'execution_id': 'exec-1',
                    'status': 'completed',
                    'started_at': '2025-07-18T18:33:35Z',
                    'completed_at': '2025-07-18T18:34:35Z',
                    'thread_id': 123
                },
                {
                    'dag_id': 'dag-2',
                    'execution_id': 'exec-2',
                    'status': 'running',
                    'started_at': '2025-07-18T18:35:35Z',
                    'completed_at': None,
                    'thread_id': 456
                }
            ]

        result = self.runner.invoke(app, ['ls'])

        assert result.exit_code == 0
        assert "Maestro DAGs" in result.output
        assert "dag-1" in result.output
        assert "dag-2" in result.output
        assert "Total DAGs: 2" in result.output

    @pytest.mark.unit
    def test_list_command_with_status_filter(self, mock_api_client, mock_check_server):
        """Test DAG listing with status filter."""
        mock_api_client.list_dags_v1.return_value = []

        result = self.runner.invoke(app, ['ls', '--filter', 'running'])

        assert result.exit_code == 0
        mock_api_client.list_dags_v1.assert_called_once_with('running')

    @pytest.mark.unit
    def test_list_command_active_flag(self, mock_api_client, mock_check_server):
        """Test DAG listing with active flag."""
        mock_api_client.list_dags_v1.return_value = []

        result = self.runner.invoke(app, ['ls', '--filter', 'active'])

        assert result.exit_code == 0
        mock_api_client.list_dags_v1.assert_called_once_with('active')

    @pytest.mark.unit
    def test_list_command_no_dags(self, mock_api_client, mock_check_server):
        """Test DAG listing when no DAGs exist."""
        mock_api_client.list_dags_v1.return_value = []

        result = self.runner.invoke(app, ['ls'])

        assert result.exit_code == 0
        assert "No DAGs found" in result.output

    # Server Commands Tests
    @pytest.mark.unit
    def test_server_status_command_running(self, mock_api_client, mock_check_server):
        """Test server status when running."""
        mock_api_client.health_check.return_value = {
            'status': 'healthy',
            'timestamp': '2025-07-18T18:33:35Z'
        }

        result = self.runner.invoke(app, ['server', 'status'])

        assert result.exit_code == 0
        assert "Server is running" in result.output
        assert "healthy" in result.output

    @pytest.mark.unit
    def test_server_status_command_not_running(self, mock_api_client, mock_check_server):
        """Test server status when not running."""
        mock_api_client.health_check.side_effect = ConnectionError("Connection failed")

        result = self.runner.invoke(app, ['server', 'status'])

        assert result.exit_code == 1
        assert "Server is not running" in result.output

    @pytest.mark.unit
    def test_server_start_command_daemon(self, mock_api_client, mock_check_server, mocker):
        """Test server start in daemon mode."""
        mock_popen = mocker.patch('maestro.cli_client.subprocess.Popen')
        mock_api_client.wait_for_server.return_value = True

        result = self.runner.invoke(app, ['server', 'start', '--daemon'])

        assert result.exit_code == 0
        assert "Maestro server started" in result.output
        mock_popen.assert_called_once()

    @pytest.mark.unit
    def test_server_start_command_daemon_fail(self, mock_api_client, mock_check_server, mocker):
        """Test server start daemon failure."""
        mock_popen = mocker.patch('maestro.cli_client.subprocess.Popen')
        mock_api_client.wait_for_server.return_value = False

        result = self.runner.invoke(app, ['server', 'start', '--daemon'])

        assert result.exit_code == 1
        assert "Failed to start server" in result.output

    @pytest.mark.unit
    def test_server_stop_command(self, mock_api_client, mock_check_server):
        """Test server stop command."""
        result = self.runner.invoke(app, ['server', 'stop'])

        assert result.exit_code == 0
        assert "Server stop command not implemented" in result.output


class TestServerConnection:
    """Test suite for server connection functionality."""

    @pytest.mark.unit
    def test_check_server_connection_success(self, mocker):
        """Test successful server connection check."""
        mock_api_client = mocker.patch('maestro.cli_client.api_client')
        mock_api_client.is_server_running.return_value = True

        # Should not raise any exception
        check_server_connection()
        mock_api_client.is_server_running.assert_called_once()

    @pytest.mark.unit
    def test_check_server_connection_failure(self, mocker):
        """Test server connection failure."""
        mock_api_client = mocker.patch('maestro.cli_client.api_client')
        mock_api_client.is_server_running.return_value = False
        mock_console = mocker.patch('maestro.cli_client.console')

        with pytest.raises(typer.Exit) as exc_info:
            check_server_connection()

        assert exc_info.value.exit_code == 1
        mock_console.print.assert_called()