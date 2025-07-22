#!/usr/bin/env python3
"""
Integration tests for the Maestro CLI client.

These tests focus on testing the interactions between different components
and edge cases that might occur in real usage scenarios.
"""

import pytest
import tempfile
import os
from unittest.mock import patch, Mock
from typer.testing import CliRunner
from maestro.cli_client import app


class TestCliIntegration:
    """Integration tests for CLI client."""
    
    def setup_method(self):
        """Set up test fixtures."""
        self.runner = CliRunner()
    
    @pytest.fixture
    def mock_api_client(self, mocker):
        """Mock API client."""
        return mocker.patch('maestro.cli_client.api_client', autospec=True)
    
    @pytest.fixture
    def mock_check_server(self, mocker):
        """Mock server connection check."""
        return mocker.patch('maestro.cli_client.check_server_connection')
    
    @pytest.fixture
    def temp_dag_file(self):
        """Create a temporary DAG file."""
        with tempfile.NamedTemporaryFile(mode='w', suffix='.yaml', delete=False) as f:
            f.write("""
dag_id: test-integration-dag
tasks:
  - task_id: task1
    type: shell
    command: echo "Hello World"
  - task_id: task2
    type: shell
    command: echo "Task 2"
    depends_on: [task1]
""")
            yield f.name
        os.unlink(f.name)
    
    @pytest.mark.integration
    def test_full_dag_workflow(self, mock_api_client, mock_check_server, temp_dag_file):
        """Test complete DAG workflow: submit -> status -> logs -> cancel."""
        # Mock responses for different stages
        submit_response = {
            'dag_id': 'test-integration-dag',
            'execution_id': 'exec-integration-123',
            'status': 'submitted',
            'submitted_at': '2025-07-18T18:33:35Z'
        }
        
        status_response = {
            'execution_id': 'exec-integration-123',
            'status': 'running',
            'started_at': '2025-07-18T18:33:35Z',
            'completed_at': None,
            'thread_id': 'thread-123',
            'tasks': [
                {
                    'task_id': 'task1',
                    'status': 'completed',
                    'started_at': '2025-07-18T18:33:35Z',
                    'completed_at': '2025-07-18T18:33:40Z'
                },
                {
                    'task_id': 'task2',
                    'status': 'running',
                    'started_at': '2025-07-18T18:33:40Z',
                    'completed_at': None
                }
            ]
        }
        
        logs_response = {
            'logs': [
                {
                    'timestamp': '2025-07-18T18:33:35Z',
                    'level': 'INFO',
                    'task_id': 'task1',
                    'message': 'Starting task1'
                },
                {
                    'timestamp': '2025-07-18T18:33:37Z',
                    'level': 'INFO',
                    'task_id': 'task1',
                    'message': 'Hello World'
                },
                {
                    'timestamp': '2025-07-18T18:33:40Z',
                    'level': 'INFO',
                    'task_id': 'task2',
                    'message': 'Starting task2'
                }
            ],
            'total_count': 3
        }
        
        cancel_response = {
            'success': True,
            'message': 'DAG cancelled successfully'
        }
        
     #   mock_api_client.submit_dag.return_value = submit_response #submit api removed
        mock_api_client.get_dag_status.return_value = status_response
        mock_api_client.get_dag_logs.return_value = logs_response
        mock_api_client.cancel_dag.return_value = cancel_response
        
        # Step 1: Submit DAG
        result = self.runner.invoke(app, ['submit', temp_dag_file])
        assert result.exit_code == 0
        assert '✓ DAG submitted successfully!' in result.output
        assert 'test-integration-dag' in result.output
        
        # Step 2: Check status
        result = self.runner.invoke(app, ['status', 'test-integration-dag'])
        assert result.exit_code == 0
        assert 'DAG Status: test-integration-dag' in result.output
        assert 'running' in result.output
        assert 'task1' in result.output
        assert 'task2' in result.output
        
        # Step 3: Get logs
        result = self.runner.invoke(app, ['logs', 'test-integration-dag'])
        assert result.exit_code == 0
        assert 'Logs: test-integration-dag' in result.output
        assert 'Hello World' in result.output
        assert 'Starting task2' in result.output
        
        # Step 4: Cancel DAG
        result = self.runner.invoke(app, ['cancel', 'test-integration-dag'])
        assert result.exit_code == 0
        assert 'DAG cancelled successfully' in result.output
        
        # Verify all API calls were made
 #       mock_api_client.submit_dag.assert_called_once()
        mock_api_client.get_dag_status.assert_called_once()
        mock_api_client.get_dag_logs.assert_called_once()
        mock_api_client.cancel_dag.assert_called_once()
    
    @pytest.mark.integration
    def test_error_handling_chain(self, mock_api_client, mock_check_server):
        """Test error handling across multiple commands."""
        # Test server connection error
        mock_check_server.side_effect = SystemExit(1)
        
        result = self.runner.invoke(app, ['status', 'test-dag'])
        assert result.exit_code == 1
        
        # Reset mock
        mock_check_server.side_effect = None
        
        # Test DAG not found error
        mock_api_client.get_dag_status.side_effect = FileNotFoundError("DAG not found")
        
        result = self.runner.invoke(app, ['status', 'nonexistent-dag'])
        assert result.exit_code == 1
        assert 'DAG execution not found' in result.output
        
        # Test API error
        mock_api_client.get_dag_status.side_effect = RuntimeError("API Error")
        
        result = self.runner.invoke(app, ['status', 'error-dag'])
        assert result.exit_code == 1
        assert 'Error: API Error' in result.output
    
    @pytest.mark.integration
    def test_dag_lifecycle_states(self, mock_api_client, mock_check_server, temp_dag_file):
        """Test DAG through different lifecycle states."""
        # Test 1: Submitted state
        # mock_api_client.submit_dag.return_value = {
        #     'dag_id': 'lifecycle-dag',
        #     'execution_id': 'exec-lifecycle',
        #     'status': 'submitted',
        #     'submitted_at': '2025-07-18T18:33:35Z'
        # }
        
        result = self.runner.invoke(app, ['submit', temp_dag_file])
        assert result.exit_code == 0
        assert 'submitted' in result.output
        
        # Test 2: Running state
        mock_api_client.get_dag_status.return_value = {
            'execution_id': 'exec-lifecycle',
            'status': 'running',
            'started_at': '2025-07-18T18:33:35Z',
            'completed_at': None,
            'thread_id': 'thread-123',
            'tasks': [
                {
                    'task_id': 'task1',
                    'status': 'running',
                    'started_at': '2025-07-18T18:33:35Z',
                    'completed_at': None
                }
            ]
        }
        
        result = self.runner.invoke(app, ['status', 'lifecycle-dag'])
        assert result.exit_code == 0
        assert 'running' in result.output
        
        # Test 3: Completed state
        mock_api_client.get_dag_status.return_value = {
            'execution_id': 'exec-lifecycle',
            'status': 'completed',
            'started_at': '2025-07-18T18:33:35Z',
            'completed_at': '2025-07-18T18:35:35Z',
            'thread_id': None,
            'tasks': [
                {
                    'task_id': 'task1',
                    'status': 'completed',
                    'started_at': '2025-07-18T18:33:35Z',
                    'completed_at': '2025-07-18T18:35:35Z'
                }
            ]
        }
        
        result = self.runner.invoke(app, ['status', 'lifecycle-dag'])
        assert result.exit_code == 0
        assert 'completed' in result.output
        
        # Test 4: Failed state
        mock_api_client.get_dag_status.return_value = {
            'execution_id': 'exec-lifecycle',
            'status': 'failed',
            'started_at': '2025-07-18T18:33:35Z',
            'completed_at': '2025-07-18T18:34:35Z',
            'thread_id': None,
            'tasks': [
                {
                    'task_id': 'task1',
                    'status': 'failed',
                    'started_at': '2025-07-18T18:33:35Z',
                    'completed_at': '2025-07-18T18:34:35Z'
                }
            ]
        }
        
        result = self.runner.invoke(app, ['status', 'lifecycle-dag'])
        assert result.exit_code == 0
        assert 'failed' in result.output
    
    @pytest.mark.integration
    def test_multiple_dag_management(self, mock_api_client, mock_check_server):
        """Test managing multiple DAGs simultaneously."""
        # Mock multiple running DAGs
        mock_api_client.get_running_dags.return_value = {
            'running_dags': [
                {
                    'dag_id': 'dag-1',
                    'execution_id': 'exec-1',
                    'started_at': '2025-07-18T18:33:35Z',
                    'thread_id': 123
                },
                {
                    'dag_id': 'dag-2',
                    'execution_id': 'exec-2',
                    'started_at': '2025-07-18T18:34:35Z',
                    'thread_id': 456
                },
                {
                    'dag_id': 'dag-3',
                    'execution_id': 'exec-3',
                    'started_at': '2025-07-18T18:35:35Z',
                    'thread_id': 789
                }
            ],
            'count': 3
        }
        
        result = self.runner.invoke(app, ['running'])
        assert result.exit_code == 0
        assert 'dag-1' in result.output
        assert 'dag-2' in result.output
        assert 'dag-3' in result.output
        assert 'Total running DAGs: 3' in result.output
        
        # Test list with different filters
        mock_api_client.list_dags.return_value = {
            'dags': [
                {
                    'dag_id': 'completed-dag',
                    'execution_id': 'exec-completed',
                    'status': 'completed',
                    'started_at': '2025-07-18T18:30:35Z',
                    'completed_at': '2025-07-18T18:32:35Z',
                    'thread_id': None
                }
            ],
            'count': 1,
            'title': 'Completed DAGs'
        }
        
        result = self.runner.invoke(app, ['list', '--status', 'completed'])
        assert result.exit_code == 0
        assert 'completed-' in result.output  # DAG ID is truncated in Rich table
        assert 'Completed DAGs' in result.output
    
    @pytest.mark.integration
    def test_dag_validation_workflow(self, mock_api_client, mock_check_server, temp_dag_file):
        """Test DAG validation before submission."""
        # Test valid DAG
        mock_api_client.validate_dag.return_value = {
            'valid': True,
            'dag_id': 'test-integration-dag',
            'tasks': [
                {
                    'task_id': 'task1',
                    'type': 'shell',
                    'dependencies': []
                },
                {
                    'task_id': 'task2',
                    'type': 'shell',
                    'dependencies': ['task1']
                }
            ],
            'total_tasks': 2
        }
        
        result = self.runner.invoke(app, ['validate', temp_dag_file])
        assert result.exit_code == 0
        assert '✓ DAG is valid' in result.output
        assert 'test-integration-dag' in result.output
        assert 'task1' in result.output
        assert 'task2' in result.output
        assert 'Total tasks: 2' in result.output
        
        # Test invalid DAG
        mock_api_client.validate_dag.return_value = {
            'valid': False,
            'error': 'Circular dependency detected between task1 and task2'
        }
        
        result = self.runner.invoke(app, ['validate', temp_dag_file])
        assert result.exit_code == 1
        assert '✗ DAG validation failed' in result.output
        assert 'Circular dependency detected' in result.output
    
    @pytest.mark.integration
    def test_server_management_workflow(self, mock_api_client, mock_check_server, mocker):
        """Test server management commands."""
        # Test server status when not running
        mock_api_client.health_check.side_effect = ConnectionError("Connection failed")
        
        result = self.runner.invoke(app, ['server', 'status'])
        assert result.exit_code == 1
        assert 'Server is not running' in result.output
        
        # Test server status when running
        mock_api_client.health_check.side_effect = None
        mock_api_client.health_check.return_value = {
            'status': 'healthy',
            'timestamp': '2025-07-18T18:33:35Z'
        }
        
        result = self.runner.invoke(app, ['server', 'status'])
        assert result.exit_code == 0
        assert 'Server is running' in result.output
        assert 'healthy' in result.output
        
        # Test server start daemon mode
        mock_popen = mocker.patch('maestro.cli_client.subprocess.Popen')
        mock_api_client.wait_for_server.return_value = True
        
        result = self.runner.invoke(app, ['server', 'start', '--daemon', '--port', '9000'])
        assert result.exit_code == 0
        assert 'Maestro server started' in result.output
        
        # Verify subprocess was called with correct arguments
        mock_popen.assert_called_once()
        call_args = mock_popen.call_args[0][0]
        assert '--port' in call_args
        assert '9000' in call_args
    
    @pytest.mark.integration
    def test_log_filtering_and_display(self, mock_api_client, mock_check_server):
        """Test log filtering and display functionality."""
        # Test logs with different levels and tasks
        mock_api_client.get_dag_logs.return_value = {
            'logs': [
                {
                    'timestamp': '2025-07-18T18:33:35Z',
                    'level': 'DEBUG',
                    'task_id': 'task1',
                    'message': 'Debug message from task1'
                },
                {
                    'timestamp': '2025-07-18T18:33:36Z',
                    'level': 'INFO',
                    'task_id': 'task1',
                    'message': 'Info message from task1'
                },
                {
                    'timestamp': '2025-07-18T18:33:37Z',
                    'level': 'WARNING',
                    'task_id': 'task2',
                    'message': 'Warning message from task2'
                },
                {
                    'timestamp': '2025-07-18T18:33:38Z',
                    'level': 'ERROR',
                    'task_id': 'task2',
                    'message': 'Error message from task2'
                }
            ],
            'total_count': 4
        }
        
        # Test with different filtering combinations
        result = self.runner.invoke(app, [
            'logs', 'test-dag',
            '--limit', '10',
            '--task', 'task1',
            '--level', 'INFO'
        ])
        
        assert result.exit_code == 0
        assert 'Logs: test-dag' in result.output
        
        # Verify API was called with correct parameters
        mock_api_client.get_dag_logs.assert_called_with(
            'test-dag', None, 10, 'task1', 'INFO'
        )
    
    @pytest.mark.integration
    def test_cleanup_workflow(self, mock_api_client, mock_check_server):
        """Test cleanup workflow with different scenarios."""
        # Test successful cleanup
        mock_api_client.cleanup_old_executions.return_value = {
            'message': 'Cleaned up 15 old executions (older than 7 days)'
        }
        
        result = self.runner.invoke(app, ['cleanup', '--days', '7'])
        assert result.exit_code == 0
        assert 'Cleaned up 15 old executions' in result.output
        
        # Test cleanup with no old executions
        mock_api_client.cleanup_old_executions.return_value = {
            'message': 'No old executions found to clean up'
        }
        
        result = self.runner.invoke(app, ['cleanup', '--days', '30'])
        assert result.exit_code == 0
        assert 'No old executions found' in result.output
        
        # Test cleanup with default days
        result = self.runner.invoke(app, ['cleanup'])
        assert result.exit_code == 0
        mock_api_client.cleanup_old_executions.assert_called_with(30)
    
    @pytest.mark.integration
    def test_edge_cases_and_error_recovery(self, mock_api_client, mock_check_server, temp_dag_file):
        """Test edge cases and error recovery scenarios."""
        # Test with very long DAG ID
        long_dag_id = 'a' * 255
        mock_api_client.get_dag_status.return_value = {
            'execution_id': 'exec-long',
            'status': 'running',
            'started_at': '2025-07-18T18:33:35Z',
            'completed_at': None,
            'thread_id': 'thread-123',
            'tasks': []
        }
        
        result = self.runner.invoke(app, ['status', long_dag_id])
        assert result.exit_code == 0
        
        # Test with empty logs
        mock_api_client.get_dag_logs.return_value = {
            'logs': [],
            'total_count': 0
        }
        
        result = self.runner.invoke(app, ['logs', 'empty-dag'])
        assert result.exit_code == 0
        assert 'No logs found' in result.output
        
        # Test with malformed timestamp
        mock_api_client.get_dag_logs.return_value = {
            'logs': [
                {
                    'timestamp': 'invalid-timestamp',
                    'level': 'INFO',
                    'task_id': 'task1',
                    'message': 'Test message'
                }
            ],
            'total_count': 1
        }
        
        result = self.runner.invoke(app, ['logs', 'malformed-dag'])
        assert result.exit_code == 0
        assert 'Test message' in result.output
        
        # Test with missing task data
        mock_api_client.get_dag_status.return_value = {
            'execution_id': 'exec-missing',
            'status': 'running',
            'started_at': '2025-07-18T18:33:35Z',
            'completed_at': None,
            'thread_id': None,
            'tasks': []
        }
        
        result = self.runner.invoke(app, ['status', 'missing-tasks-dag'])
        assert result.exit_code == 0
        # Should handle missing tasks gracefully
