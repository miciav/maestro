#!/usr/bin/env python3
"""
Shared pytest fixtures and test utilities for the Maestro test suite.

This module provides common fixtures, mock objects, and utility functions
that can be used across multiple test files.
"""
import shutil
from pathlib import Path

import pytest
import tempfile
import os
import sqlite3
from unittest.mock import Mock, MagicMock, patch
from datetime import datetime


def create_test_db():
    """Create an in-memory SQLite database for testing with actual schema."""
    conn = sqlite3.connect(':memory:')
    conn.execute("PRAGMA journal_mode=WAL")  # Enable WAL mode for better concurrency
    conn.execute("PRAGMA foreign_keys = ON")  # Enable foreign key support

    # Create the actual database schema from StatusManager
    conn.executescript('''
                       -- Main table for DAGs
                       CREATE TABLE IF NOT EXISTS dags
                       (
                           id
                           TEXT
                           PRIMARY
                           KEY,
                           created_at
                           TIMESTAMP
                           DEFAULT
                           CURRENT_TIMESTAMP
                       );

                       -- Main table for executions (must be created before tasks)
                       CREATE TABLE IF NOT EXISTS executions
                       (
                           id
                           TEXT,
                           dag_id
                           TEXT,
                           status
                           TEXT,
                           started_at
                           TIMESTAMP,
                           completed_at
                           TIMESTAMP,
                           thread_id
                           TEXT,
                           pid
                           INTEGER,
                           PRIMARY
                           KEY
                       (
                           id,
                           dag_id
                       ),
                           FOREIGN KEY
                       (
                           dag_id
                       ) REFERENCES dags
                       (
                           id
                       ) ON DELETE CASCADE
                           );

                       -- Main table for tasks (references executions table)
                       CREATE TABLE IF NOT EXISTS tasks
                       (
                           id
                           TEXT,
                           dag_id
                           TEXT,
                           execution_id
                           TEXT,
                           status
                           TEXT,
                           started_at
                           TIMESTAMP,
                           completed_at
                           TIMESTAMP,
                           thread_id
                           TEXT,
                           PRIMARY
                           KEY
                       (
                           id,
                           dag_id,
                           execution_id
                       ),
                           FOREIGN KEY
                       (
                           dag_id
                       ) REFERENCES dags
                       (
                           id
                       ) ON DELETE CASCADE,
                           FOREIGN KEY
                       (
                           execution_id,
                           dag_id
                       ) REFERENCES executions
                       (
                           id,
                           dag_id
                       )
                         ON DELETE CASCADE
                           );

                       -- Main table for logs
                       CREATE TABLE IF NOT EXISTS logs
                       (
                           id
                           INTEGER
                           PRIMARY
                           KEY
                           AUTOINCREMENT,
                           dag_id
                           TEXT,
                           execution_id
                           TEXT,
                           task_id
                           TEXT,
                           level
                           TEXT,
                           message
                           TEXT,
                           timestamp
                           TIMESTAMP,
                           thread_id
                           TEXT,
                           FOREIGN
                           KEY
                       (
                           dag_id
                       ) REFERENCES dags
                       (
                           id
                       ) ON DELETE CASCADE
                           );
                       ''')

    conn.commit()
    return conn


@pytest.fixture(scope="session")
def sample_dag_content():
    """Sample DAG YAML content for testing."""
    return """
dag_id: test-dag
description: A test DAG for unit testing
schedule_interval: "@daily"
start_date: "2025-01-01"

tasks:
  - task_id: task1
    type: shell
    command: echo "Hello from task1"

  - task_id: task2
    type: shell
    command: echo "Hello from task2"
    depends_on: [task1]

  - task_id: task3
    type: python
    script: |
      print("Python task execution")
      return "success"
    depends_on: [task1]

  - task_id: task4
    type: shell
    command: echo "Final task"
    depends_on: [task2, task3]
"""


@pytest.fixture
def temp_dag_file(sample_dag_content):
    """Create a temporary DAG file for testing."""
    with tempfile.NamedTemporaryFile(mode='w', suffix='.yaml', delete=False) as f:
        f.write(sample_dag_content)
        yield f.name
    os.unlink(f.name)


@pytest.fixture
def mock_dag_response():
    """Standard mock DAG response."""
    return {
        'dag_id': 'test-dag',
        'execution_id': 'exec-12345',
        'status': 'submitted',
        'submitted_at': '2025-07-18T18:33:35Z'
    }


@pytest.fixture
def mock_status_response():
    """Standard mock status response."""
    return {
        'execution_id': 'exec-12345',
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


@pytest.fixture
def mock_logs_response():
    """Standard mock logs response."""
    return {
        'logs': [
            {
                'timestamp': '2025-07-18T18:33:35Z',
                'level': 'INFO',
                'task_id': 'task1',
                'message': 'Task1 started'
            },
            {
                'timestamp': '2025-07-18T18:33:37Z',
                'level': 'INFO',
                'task_id': 'task1',
                'message': 'Hello from task1'
            },
            {
                'timestamp': '2025-07-18T18:33:40Z',
                'level': 'INFO',
                'task_id': 'task1',
                'message': 'Task1 completed'
            },
            {
                'timestamp': '2025-07-18T18:33:40Z',
                'level': 'INFO',
                'task_id': 'task2',
                'message': 'Task2 started'
            }
        ],
        'total_count': 4
    }


@pytest.fixture
def mock_running_dags_response():
    """Standard mock running DAGs response."""
    return {
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
            }
        ],
        'count': 2
    }


@pytest.fixture
def mock_validation_response():
    """Standard mock validation response."""
    return {
        'valid': True,
        'dag_id': 'test-dag',
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
            },
            {
                'task_id': 'task3',
                'type': 'python',
                'dependencies': ['task1']
            },
            {
                'task_id': 'task4',
                'type': 'shell',
                'dependencies': ['task2', 'task3']
            }
        ],
        'total_tasks': 4
    }


@pytest.fixture
def mock_api_client_full():
    """Fully configured mock API client."""
    mock_client = Mock()

    # Configure default return values
    mock_client.health_check.return_value = {
        'status': 'healthy',
        'timestamp': '2025-07-18T18:33:35Z'
    }

    mock_client.submit_dag.return_value = {
        'dag_id': 'test-dag',
        'execution_id': 'exec-12345',
        'status': 'submitted',
        'submitted_at': '2025-07-18T18:33:35Z'
    }

    mock_client.get_dag_status.return_value = {
        'execution_id': 'exec-12345',
        'status': 'running',
        'started_at': '2025-07-18T18:33:35Z',
        'completed_at': None,
        'thread_id': 'thread-123',
        'tasks': []
    }

    mock_client.get_dag_logs.return_value = {
        'logs': [],
        'total_count': 0
    }

    mock_client.get_running_dags.return_value = {
        'running_dags': [],
        'count': 0
    }

    mock_client.cancel_dag.return_value = {
        'success': True,
        'message': 'DAG cancelled successfully'
    }

    mock_client.validate_dag.return_value = {
        'valid': True,
        'dag_id': 'test-dag',
        'tasks': [],
        'total_tasks': 0
    }

    mock_client.cleanup_old_executions.return_value = {
        'message': 'Cleaned up 0 old executions'
    }

    mock_client.list_dags.return_value = {
        'dags': [],
        'count': 0,
        'title': 'All DAGs'
    }

    mock_client.is_server_running.return_value = True
    mock_client.wait_for_server.return_value = True

    return mock_client


class MockAPIClientBuilder:
    """Builder class for creating customized mock API clients."""

    def __init__(self):
        self.client = Mock()
        self._setup_defaults()

    def _setup_defaults(self):
        """Set up default mock responses."""
        self.client.health_check.return_value = {'status': 'healthy'}
        self.client.is_server_running.return_value = True
        self.client.wait_for_server.return_value = True

    def with_submit_response(self, **kwargs):
        """Configure submit DAG response."""
        default_response = {
            'dag_id': 'test-dag',
            'execution_id': 'exec-12345',
            'status': 'submitted',
            'submitted_at': '2025-07-18T18:33:35Z'
        }
        default_response.update(kwargs)
        self.client.submit_dag.return_value = default_response
        return self

    def with_status_response(self, **kwargs):
        """Configure status response."""
        default_response = {
            'execution_id': 'exec-12345',
            'status': 'running',
            'started_at': '2025-07-18T18:33:35Z',
            'completed_at': None,
            'thread_id': 'thread-123',
            'tasks': []
        }
        default_response.update(kwargs)
        self.client.get_dag_status.return_value = default_response
        return self

    def with_logs_response(self, logs=None, **kwargs):
        """Configure logs response."""
        default_response = {
            'logs': logs or [],
            'total_count': len(logs) if logs else 0
        }
        default_response.update(kwargs)
        self.client.get_dag_logs.return_value = default_response
        return self

    def with_validation_response(self, valid=True, **kwargs):
        """Configure validation response."""
        if valid:
            default_response = {
                'valid': True,
                'dag_id': 'test-dag',
                'tasks': [],
                'total_tasks': 0
            }
        else:
            default_response = {
                'valid': False,
                'error': 'Validation failed'
            }
        default_response.update(kwargs)
        self.client.validate_dag.return_value = default_response
        return self

    def with_error(self, method_name, exception):
        """Configure a method to raise an exception."""
        getattr(self.client, method_name).side_effect = exception
        return self

    def build(self):
        """Return the configured mock client."""
        return self.client


@pytest.fixture
def mock_client_builder():
    """Factory for creating customized mock API clients."""
    return MockAPIClientBuilder


@pytest.fixture(scope="session")
def temp_dir():
    """Create a temporary directory for test files."""
    temp_dir = tempfile.mkdtemp()
    yield Path(temp_dir)
    shutil.rmtree(temp_dir)


@pytest.fixture(autouse=True)
def isolate_database():
    """Ensure each test gets a clean database state."""
    # Create a unique temporary database for each test
    import tempfile
    with tempfile.NamedTemporaryFile(suffix='.db', delete=False) as temp_db:
        temp_db_path = temp_db.name
    
    # Mock the StatusManager to use our temp database
    with patch('maestro.core.status_manager.StatusManager') as mock_sm_class:
        # Create a real StatusManager instance with the temp database
        from maestro.core.status_manager import StatusManager
        real_sm = StatusManager(temp_db_path)
        mock_sm_class.return_value = real_sm
        
        yield
        
        # Cleanup the temp database file
        try:
            os.unlink(temp_db_path)
        except OSError:
            pass


@pytest.fixture
def mock_terraform():
    """Mock terraform availability for tests."""
    with patch('shutil.which') as mock_which:
        mock_which.return_value = '/usr/bin/terraform'
        yield mock_which


def create_sample_log_entries(count=5, task_id='task1', level='INFO'):
    """Create sample log entries for testing."""
    entries = []
    for i in range(count):
        entries.append({
            'timestamp': f'2025-07-18T18:33:{35 + i:02d}Z',
            'level': level,
            'task_id': task_id,
            'message': f'Log message {i + 1} from {task_id}'
        })
    return entries


def create_sample_tasks(count=3, status='running'):
    """Create sample task objects for testing."""
    tasks = []
    for i in range(count):
        task = {
            'task_id': f'task{i + 1}',
            'status': status,
            'started_at': f'2025-07-18T18:33:{35 + i:02d}Z',
            'completed_at': f'2025-07-18T18:34:{35 + i:02d}Z' if status == 'completed' else None
        }
        tasks.append(task)
    return tasks


def assert_command_success(result, expected_messages=None):
    """Assert that a CLI command executed successfully."""
    assert result.exit_code == 0, f"Command failed with output: {result.output}"

    if expected_messages:
        for message in expected_messages:
            assert message in result.output, f"Expected message '{message}' not found in output: {result.output}"


def assert_command_error(result, expected_error=None, expected_exit_code=1):
    """Assert that a CLI command failed with expected error."""
    assert result.exit_code == expected_exit_code, f"Expected exit code {expected_exit_code}, got {result.exit_code}"

    if expected_error:
        assert expected_error in result.output, f"Expected error '{expected_error}' not found in output: {result.output}"


# Test data constants
TEST_DAG_ID = 'test-dag'
TEST_EXECUTION_ID = 'exec-12345'
TEST_THREAD_ID = 'thread-123'
TEST_TIMESTAMP = '2025-07-18T18:33:35Z'

# Common test scenarios
COMMON_ERROR_SCENARIOS = [
    ('connection_error', ConnectionError("Could not connect to server")),
    ('timeout_error', TimeoutError("Request timed out")),
    ('not_found_error', FileNotFoundError("Resource not found")),
    ('value_error', ValueError("Invalid input")),
    ('runtime_error', RuntimeError("Server error"))
]

# Sample DAG configurations for different test scenarios
SAMPLE_DAG_CONFIGS = {
    'simple': {
        'dag_id': 'simple-dag',
        'tasks': [
            {'task_id': 'task1', 'type': 'shell', 'command': 'echo "hello"'}
        ]
    },
    'complex': {
        'dag_id': 'complex-dag',
        'tasks': [
            {'task_id': 'task1', 'type': 'shell', 'command': 'echo "task1"'},
            {'task_id': 'task2', 'type': 'shell', 'command': 'echo "task2"', 'depends_on': ['task1']},
            {'task_id': 'task3', 'type': 'python', 'script': 'print("task3")', 'depends_on': ['task1']},
            {'task_id': 'task4', 'type': 'shell', 'command': 'echo "task4"', 'depends_on': ['task2', 'task3']}
        ]
    },
    'invalid': {
        'dag_id': 'invalid-dag',
        'tasks': [
            {'task_id': 'task1', 'type': 'shell', 'command': 'echo "task1"', 'depends_on': ['task2']},
            {'task_id': 'task2', 'type': 'shell', 'command': 'echo "task2"', 'depends_on': ['task1']}
        ]
    }
}