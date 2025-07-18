#!/usr/bin/env python3
"""
Test suite for the attach command and streaming functionality.

This test suite focuses on the complex attach command which involves
signal handling, streaming, and user interaction.
"""

import pytest
import signal
import time
import unittest.mock
from unittest.mock import patch
from typer.testing import CliRunner
from maestro.cli_client import app


class TestAttachCommand:
    """Test suite for the attach command."""
    
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
    def mock_signal(self, mocker):
        """Mock signal handling."""
        return mocker.patch('maestro.cli_client.signal')
    
    @pytest.mark.unit
    def test_attach_command_success(self, mock_api_client, mock_check_server, mock_signal):
        """Test successful attach command execution."""
        # Mock streaming logs generator
        mock_logs = [
            {
                "timestamp": "2025-07-18T18:33:35.123456Z",
                "level": "INFO",
                "task_id": "task-1",
                "message": "Starting task"
            },
            {
                "timestamp": "2025-07-18T18:33:36.123456Z",
                "level": "ERROR",
                "task_id": "task-2",
                "message": "Task failed"
            }
        ]
        
        mock_api_client.stream_dag_logs.return_value = iter(mock_logs)
        
        result = self.runner.invoke(app, ['attach', 'test-dag'])
        
        assert result.exit_code == 0
        assert "Attaching to live logs for DAG: test-dag" in result.output
        assert "Press Ctrl+C to detach" in result.output
        assert "Starting task" in result.output
        assert "Task failed" in result.output
        
        # Verify signal handlers were set
        assert mock_signal.signal.call_count == 2
        mock_signal.signal.assert_any_call(signal.SIGINT, unittest.mock.ANY)
        mock_signal.signal.assert_any_call(signal.SIGTERM, unittest.mock.ANY)
    
    @pytest.mark.unit
    def test_attach_command_with_filters(self, mock_api_client, mock_check_server, mock_signal):
        """Test attach command with filters."""
        mock_api_client.stream_dag_logs.return_value = iter([])
        
        result = self.runner.invoke(app, [
            'attach', 'test-dag',
            '--execution-id', 'exec-123',
            '--task', 'specific-task',
            '--level', 'ERROR'
        ])
        
        assert result.exit_code == 0
        mock_api_client.stream_dag_logs.assert_called_once_with(
            'test-dag', 'exec-123', 'specific-task', 'ERROR'
        )
    
    @pytest.mark.unit
    def test_attach_command_with_execution_id(self, mock_api_client, mock_check_server, mock_signal):
        """Test attach command with execution ID."""
        mock_api_client.stream_dag_logs.return_value = iter([])
        
        result = self.runner.invoke(app, [
            'attach', 'test-dag',
            '--execution-id', 'exec-specific'
        ])
        
        assert result.exit_code == 0
        assert "Execution ID: exec-specific" in result.output
    
    @pytest.mark.unit
    def test_attach_command_stream_error(self, mock_api_client, mock_check_server, mock_signal):
        """Test attach command with stream error."""
        mock_logs = [
            {"error": "Stream connection lost"}
        ]
        
        mock_api_client.stream_dag_logs.return_value = iter(mock_logs)
        
        result = self.runner.invoke(app, ['attach', 'test-dag'])
        
        assert result.exit_code == 0
        assert "Stream error: Stream connection lost" in result.output
    
    @pytest.mark.unit
    def test_attach_command_keyboard_interrupt(self, mock_api_client, mock_check_server, mock_signal):
        """Test attach command with keyboard interrupt."""
        def mock_stream_logs(*args, **kwargs):
            """Mock streaming that raises KeyboardInterrupt."""
            yield {"timestamp": "2025-07-18T18:33:35Z", "level": "INFO", "task_id": "task-1", "message": "Starting"}
            raise KeyboardInterrupt("User interrupted")
        
        mock_api_client.stream_dag_logs.side_effect = mock_stream_logs
        
        result = self.runner.invoke(app, ['attach', 'test-dag'])
        
        assert result.exit_code == 0
        assert "Detached from log stream" in result.output
    
    @pytest.mark.unit
    def test_attach_command_general_exception(self, mock_api_client, mock_check_server, mock_signal):
        """Test attach command with general exception."""
        mock_api_client.stream_dag_logs.side_effect = RuntimeError("Stream error")
        
        result = self.runner.invoke(app, ['attach', 'test-dag'])
        
        assert result.exit_code == 1
        assert "Error: Stream error" in result.output
    
    @pytest.mark.unit
    def test_attach_command_custom_server_url(self, mock_api_client, mock_check_server, mock_signal):
        """Test attach command with custom server URL."""
        mock_api_client.stream_dag_logs.return_value = iter([])
        
        result = self.runner.invoke(app, [
            'attach', 'test-dag',
            '--server', 'http://custom:9000'
        ])
        
        assert result.exit_code == 0
        # Verify that the API client base_url was updated
        assert mock_api_client.base_url == 'http://custom:9000'
    
    @pytest.mark.unit
    def test_attach_command_log_timestamp_parsing(self, mock_api_client, mock_check_server, mock_signal):
        """Test attach command log timestamp parsing."""
        mock_logs = [
            {
                "timestamp": "2025-07-18T18:33:35.123456Z",
                "level": "INFO",
                "task_id": "task-1",
                "message": "Test with full timestamp"
            },
            {
                "timestamp": "18:33:35",
                "level": "DEBUG",
                "task_id": "task-2",
                "message": "Test with time only"
            }
        ]
        
        mock_api_client.stream_dag_logs.return_value = iter(mock_logs)
        
        result = self.runner.invoke(app, ['attach', 'test-dag'])
        
        assert result.exit_code == 0
        assert "18:33:35" in result.output
        assert "Test with full timestamp" in result.output
        assert "Test with time only" in result.output
    
    @pytest.mark.unit
    def test_attach_command_log_level_styling(self, mock_api_client, mock_check_server, mock_signal):
        """Test attach command log level styling."""
        mock_logs = [
            {
                "timestamp": "2025-07-18T18:33:35Z",
                "level": "ERROR",
                "task_id": "task-1",
                "message": "Error message"
            },
            {
                "timestamp": "2025-07-18T18:33:36Z",
                "level": "WARNING",
                "task_id": "task-2",
                "message": "Warning message"
            },
            {
                "timestamp": "2025-07-18T18:33:37Z",
                "level": "INFO",
                "task_id": "task-3",
                "message": "Info message"
            },
            {
                "timestamp": "2025-07-18T18:33:38Z",
                "level": "DEBUG",
                "task_id": "task-4",
                "message": "Debug message"
            },
            {
                "timestamp": "2025-07-18T18:33:39Z",
                "level": "UNKNOWN",
                "task_id": "task-5",
                "message": "Unknown level message"
            }
        ]
        
        mock_api_client.stream_dag_logs.return_value = iter(mock_logs)
        
        result = self.runner.invoke(app, ['attach', 'test-dag'])
        
        assert result.exit_code == 0
        # All messages should be present
        assert "Error message" in result.output
        assert "Warning message" in result.output
        assert "Info message" in result.output
        assert "Debug message" in result.output
        assert "Unknown level message" in result.output


class TestStreamingIntegration:
    """Integration tests for streaming functionality."""
    
    @pytest.mark.integration
    def test_attach_command_integration(self, mocker):
        """Test attach command integration with API client."""
        runner = CliRunner()
        
        # Mock the API client's stream method to return a controlled stream
        mock_api_client = mocker.patch('maestro.cli_client.api_client')
        mock_check_server = mocker.patch('maestro.cli_client.check_server_connection')
        
        # Create a controlled stream that ends after a few messages
        def controlled_stream(*args, **kwargs):
            messages = [
                {
                    "timestamp": "2025-07-18T18:33:35Z",
                    "level": "INFO",
                    "task_id": "task-1",
                    "message": "Task started"
                },
                {
                    "timestamp": "2025-07-18T18:33:36Z",
                    "level": "INFO",
                    "task_id": "task-1",
                    "message": "Task running"
                },
                {
                    "timestamp": "2025-07-18T18:33:37Z",
                    "level": "INFO",
                    "task_id": "task-1",
                    "message": "Task completed"
                }
            ]
            
            for msg in messages:
                yield msg
        
        mock_api_client.stream_dag_logs.side_effect = controlled_stream
        
        result = runner.invoke(app, ['attach', 'test-dag-integration'])
        
        assert result.exit_code == 0
        assert "Attaching to live logs for DAG: test-dag-integration" in result.output
        assert "Task started" in result.output
        assert "Task running" in result.output
        assert "Task completed" in result.output
    
    @pytest.mark.slow
    def test_attach_command_long_running_stream(self, mocker):
        """Test attach command with a longer running stream."""
        runner = CliRunner()
        
        mock_api_client = mocker.patch('maestro.cli_client.api_client')
        mock_check_server = mocker.patch('maestro.cli_client.check_server_connection')
        
        # Simulate a longer running stream
        def long_stream(*args, **kwargs):
            for i in range(10):
                yield {
                    "timestamp": f"2025-07-18T18:33:{35+i:02d}Z",
                    "level": "INFO",
                    "task_id": f"task-{i+1}",
                    "message": f"Processing item {i+1}"
                }
                # Small delay to simulate real streaming
                time.sleep(0.01)
        
        mock_api_client.stream_dag_logs.side_effect = long_stream
        
        result = runner.invoke(app, ['attach', 'test-dag-long'])
        
        assert result.exit_code == 0
        assert "Processing item 1" in result.output
        assert "Processing item 10" in result.output
