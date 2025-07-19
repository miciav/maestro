#!/usr/bin/env python3
"""
Comprehensive test suite for the Maestro API client.

This test suite ensures high coverage of all API client methods and error scenarios,
using the responses library to mock HTTP calls.
"""

import pytest
import responses
import requests
import json
from unittest.mock import patch, Mock
from maestro.client.api_client import MaestroAPIClient


class TestMaestroAPIClient:
    """Test suite for MaestroAPIClient."""
    
    def setup_method(self):
        """Set up test fixtures."""
        self.client = MaestroAPIClient(base_url="http://localhost:8000")
        self.mock_dag_response = {
            "dag_id": "test-dag",
            "execution_id": "exec-123",
            "status": "submitted",
            "submitted_at": "2025-07-18T18:33:35Z"
        }
    
    @pytest.fixture
    def mock_responses(self):
        """Mock HTTP responses."""
        with responses.RequestsMock() as rsps:
            yield rsps
    
    # Constructor Tests
    @pytest.mark.unit
    def test_client_initialization(self):
        """Test client initialization with default and custom values."""
        # Default initialization
        client = MaestroAPIClient()
        assert client.base_url == "http://localhost:8000"
        assert client.timeout == 30
        
        # Custom initialization
        client = MaestroAPIClient(base_url="http://custom:9000", timeout=60)
        assert client.base_url == "http://custom:9000"
        assert client.timeout == 60
    
    @pytest.mark.unit
    def test_client_initialization_strips_trailing_slash(self):
        """Test that trailing slashes are stripped from base_url."""
        client = MaestroAPIClient(base_url="http://localhost:8000/")
        assert client.base_url == "http://localhost:8000"
    
    # Health Check Tests
    @pytest.mark.unit
    def test_health_check_success(self, mock_responses):
        """Test successful health check."""
        mock_responses.add(
            responses.GET,
            "http://localhost:8000/",
            json={"status": "healthy", "timestamp": "2025-07-18T18:33:35Z"},
            status=200
        )
        
        result = self.client.health_check()
        
        assert result["status"] == "healthy"
        assert result["timestamp"] == "2025-07-18T18:33:35Z"
        assert len(mock_responses.calls) == 1
    
    @pytest.mark.unit
    def test_health_check_connection_error(self, mock_responses):
        """Test health check with connection error."""
        mock_responses.add(
            responses.GET,
            "http://localhost:8000/",
            body=requests.exceptions.ConnectionError("Connection failed")
        )
        
        with pytest.raises(ConnectionError) as exc_info:
            self.client.health_check()
        
        assert "Could not connect to Maestro server" in str(exc_info.value)
    
    # Submit DAG Tests
    @pytest.mark.unit
    def test_submit_dag_success(self, mock_responses):
        """Test successful DAG submission."""
        mock_responses.add(
            responses.POST,
            "http://localhost:8000/dags/submit",
            json=self.mock_dag_response,
            status=200
        )
        
        result = self.client.submit_dag("/path/to/dag.yaml",
                                        resume=True,
                                        fail_fast=False)
        
        assert result == self.mock_dag_response
        assert len(mock_responses.calls) == 1
        
        # Verify request payload
        request_body = json.loads(mock_responses.calls[0].request.body)
        assert request_body["dag_file_path"] == "/path/to/dag.yaml"
        assert request_body["resume"] is True
        assert request_body["fail_fast"] is False
    
    @pytest.mark.unit
    def test_submit_dag_default_parameters(self, mock_responses):
        """Test DAG submission with default parameters."""
        mock_responses.add(
            responses.POST,
            "http://localhost:8000/dags/submit",
            json=self.mock_dag_response,
            status=200
        )
        
        result = self.client.submit_dag("/path/to/dag.yaml")
        
        request_body = json.loads(mock_responses.calls[0].request.body)
        assert request_body["resume"] is False
        assert request_body["fail_fast"] is True
    
    @pytest.mark.unit
    def test_submit_dag_bad_request(self, mock_responses):
        """Test DAG submission with bad request."""
        mock_responses.add(
            responses.POST,
            "http://localhost:8000/dags/submit",
            json={"detail": "Invalid DAG file"},
            status=400
        )
        
        with pytest.raises(ValueError) as exc_info:
            self.client.submit_dag("/path/to/invalid.yaml")
        
        assert "Bad request: Invalid DAG file" in str(exc_info.value)
    
    # Get DAG Status Tests
    @pytest.mark.unit
    def test_get_dag_status_success(self, mock_responses):
        """Test successful DAG status retrieval."""
        status_response = {
            "execution_id": "exec-123",
            "status": "running",
            "started_at": "2025-07-18T18:33:35Z",
            "completed_at": None,
            "thread_id": "thread-1",
            "tasks": []
        }
        
        mock_responses.add(
            responses.GET,
            "http://localhost:8000/dags/test-dag/status",
            json=status_response,
            status=200
        )
        
        result = self.client.get_dag_status("test-dag")
        
        assert result == status_response
        assert len(mock_responses.calls) == 1
    
    @pytest.mark.unit
    def test_get_dag_status_with_execution_id(self, mock_responses):
        """Test DAG status retrieval with execution ID."""
        mock_responses.add(
            responses.GET,
            "http://localhost:8000/dags/test-dag/status",
            json={"execution_id": "exec-specific"},
            status=200
        )
        
        result = self.client.get_dag_status("test-dag", execution_id="exec-specific")
        
        # Check that execution_id parameter was passed
        assert mock_responses.calls[0].request.url.endswith("?execution_id=exec-specific")
    
    @pytest.mark.unit
    def test_get_dag_status_not_found(self, mock_responses):
        """Test DAG status retrieval for non-existent DAG."""
        mock_responses.add(
            responses.GET,
            "http://localhost:8000/dags/nonexistent/status",
            status=404
        )
        
        with pytest.raises(FileNotFoundError) as exc_info:
            self.client.get_dag_status("nonexistent")
        
        assert "Resource not found" in str(exc_info.value)
    
    # Get DAG Logs Tests
    @pytest.mark.unit
    def test_get_dag_logs_success(self, mock_responses):
        """Test successful DAG logs retrieval."""
        logs_response = {
            "logs": [
                {
                    "timestamp": "2025-07-18T18:33:35Z",
                    "level": "INFO",
                    "task_id": "task-1",
                    "message": "Task started"
                }
            ],
            "total_count": 1
        }
        
        mock_responses.add(
            responses.GET,
            "http://localhost:8000/dags/test-dag/logs",
            json=logs_response,
            status=200
        )
        
        result = self.client.get_dag_logs("test-dag")
        
        assert result == logs_response
        assert len(mock_responses.calls) == 1
    
    @pytest.mark.unit
    def test_get_dag_logs_with_filters(self, mock_responses):
        """Test DAG logs retrieval with filters."""
        mock_responses.add(
            responses.GET,
            "http://localhost:8000/dags/test-dag/logs",
            json={"logs": [], "total_count": 0},
            status=200
        )
        
        result = self.client.get_dag_logs(
            "test-dag",
            execution_id="exec-123",
            limit=50,
            task_filter="specific-task",
            level_filter="ERROR"
        )
        
        # Check URL parameters
        request_url = mock_responses.calls[0].request.url
        assert "limit=50" in request_url
        assert "execution_id=exec-123" in request_url
        assert "task_filter=specific-task" in request_url
        assert "level_filter=ERROR" in request_url
    
    # Stream DAG Logs Tests
    @pytest.mark.unit
    def test_stream_dag_logs_success(self, mock_responses):
        """Test successful DAG logs streaming."""
        # Mock SSE stream response
        stream_data = [
            "data: {\"timestamp\": \"2025-07-18T18:33:35Z\", \"level\": \"INFO\", \"task_id\": \"task-1\", \"message\": \"Log 1\"}\n",
            "data: {\"timestamp\": \"2025-07-18T18:33:36Z\", \"level\": \"ERROR\", \"task_id\": \"task-2\", \"message\": \"Log 2\"}\n"
        ]
        
        mock_responses.add(
            responses.GET,
            "http://localhost:8000/dags/test-dag/logs/stream",
            body="".join(stream_data),
            status=200,
            stream=True
        )
        
        logs = list(self.client.stream_dag_logs("test-dag"))
        
        assert len(logs) == 2
        assert logs[0]["message"] == "Log 1"
        assert logs[1]["message"] == "Log 2"
    
    @pytest.mark.unit
    def test_stream_dag_logs_with_filters(self, mock_responses):
        """Test DAG logs streaming with filters."""
        mock_responses.add(
            responses.GET,
            "http://localhost:8000/dags/test-dag/logs/stream",
            body="",
            status=200,
            stream=True
        )
        
        list(self.client.stream_dag_logs(
            "test-dag",
            execution_id="exec-123",
            task_filter="specific-task",
            level_filter="ERROR"
        ))
        
        # Check URL parameters
        request_url = mock_responses.calls[0].request.url
        assert "execution_id=exec-123" in request_url
        assert "task_filter=specific-task" in request_url
        assert "level_filter=ERROR" in request_url
    
    @pytest.mark.unit
    def test_stream_dag_logs_invalid_json(self, mock_responses):
        """Test DAG logs streaming with invalid JSON."""
        mock_responses.add(
            responses.GET,
            "http://localhost:8000/dags/test-dag/logs/stream",
            body="data: {invalid json}\n",
            status=200,
            stream=True
        )
        
        logs = list(self.client.stream_dag_logs("test-dag"))
        
        # Should skip invalid JSON lines
        assert len(logs) == 0
    
    # Get Running DAGs Tests
    @pytest.mark.unit
    def test_get_running_dags_success(self, mock_responses):
        """Test successful running DAGs retrieval."""
        running_response = {
            "running_dags": [
                {
                    "dag_id": "dag-1",
                    "execution_id": "exec-1",
                    "started_at": "2025-07-18T18:33:35Z",
                    "thread_id": 123
                }
            ],
            "count": 1
        }
        
        mock_responses.add(
            responses.GET,
            "http://localhost:8000/dags/running",
            json=running_response,
            status=200
        )
        
        result = self.client.get_running_dags()
        
        assert result == running_response
        assert len(mock_responses.calls) == 1
    
    # Cancel DAG Tests
    @pytest.mark.unit
    def test_cancel_dag_success(self, mock_responses):
        """Test successful DAG cancellation."""
        cancel_response = {
            "success": True,
            "message": "DAG cancelled successfully"
        }
        
        mock_responses.add(
            responses.POST,
            "http://localhost:8000/dags/test-dag/cancel",
            json=cancel_response,
            status=200
        )
        
        result = self.client.cancel_dag("test-dag")
        
        assert result == cancel_response
        assert len(mock_responses.calls) == 1
    
    @pytest.mark.unit
    def test_cancel_dag_with_execution_id(self, mock_responses):
        """Test DAG cancellation with execution ID."""
        mock_responses.add(
            responses.POST,
            "http://localhost:8000/dags/test-dag/cancel",
            json={"success": True},
            status=200
        )
        
        result = self.client.cancel_dag("test-dag", execution_id="exec-123")
        
        # Check that execution_id parameter was passed
        assert mock_responses.calls[0].request.url.endswith("?execution_id=exec-123")
    
    # Validate DAG Tests
    @pytest.mark.unit
    def test_validate_dag_success(self, mock_responses):
        """Test successful DAG validation."""
        validate_response = {
            "valid": True,
            "dag_id": "test-dag",
            "tasks": [
                {
                    "task_id": "task-1",
                    "type": "python",
                    "dependencies": []
                }
            ],
            "total_tasks": 1
        }
        
        mock_responses.add(
            responses.POST,
            "http://localhost:8000/dags/validate",
            json=validate_response,
            status=200
        )
        
        result = self.client.validate_dag("/path/to/dag.yaml")
        
        assert result == validate_response
        assert len(mock_responses.calls) == 1
        
        # Verify request payload
        request_body = json.loads(mock_responses.calls[0].request.body)
        assert request_body["dag_file_path"] == "/path/to/dag.yaml"
    
    @pytest.mark.unit
    def test_validate_dag_invalid(self, mock_responses):
        """Test DAG validation failure."""
        validate_response = {
            "valid": False,
            "error": "Invalid DAG structure"
        }
        
        mock_responses.add(
            responses.POST,
            "http://localhost:8000/dags/validate",
            json=validate_response,
            status=200
        )
        
        result = self.client.validate_dag("/path/to/invalid.yaml")
        
        assert result == validate_response
        assert not result["valid"]
    
    # Cleanup Tests
    @pytest.mark.unit
    def test_cleanup_old_executions_success(self, mock_responses):
        """Test successful cleanup of old executions."""
        cleanup_response = {
            "message": "Cleaned up 5 old executions"
        }
        
        mock_responses.add(
            responses.DELETE,
            "http://localhost:8000/dags/cleanup",
            json=cleanup_response,
            status=200
        )
        
        result = self.client.cleanup_old_executions(days=7)
        
        assert result == cleanup_response
        assert len(mock_responses.calls) == 1
        
        # Check that days parameter was passed
        assert mock_responses.calls[0].request.url.endswith("?days=7")
    
    @pytest.mark.unit
    def test_cleanup_old_executions_default_days(self, mock_responses):
        """Test cleanup with default days parameter."""
        mock_responses.add(
            responses.DELETE,
            "http://localhost:8000/dags/cleanup",
            json={"message": "Cleaned up"},
            status=200
        )
        
        result = self.client.cleanup_old_executions()
        
        # Check that default days=30 was used
        assert mock_responses.calls[0].request.url.endswith("?days=30")
    
    # List DAGs Tests
    @pytest.mark.unit
    def test_list_dags_success(self, mock_responses):
        """Test successful DAG listing."""
        list_response = {
            "dags": [
                {
                    "dag_id": "dag-1",
                    "execution_id": "exec-1",
                    "status": "completed",
                    "started_at": "2025-07-18T18:33:35Z",
                    "completed_at": "2025-07-18T18:34:35Z"
                }
            ],
            "count": 1,
            "title": "All DAGs"
        }
        
        mock_responses.add(
            responses.GET,
            "http://localhost:8000/dags/list",
            json=list_response,
            status=200
        )
        
        result = self.client.list_dags()
        
        assert result == list_response
        assert len(mock_responses.calls) == 1
    
    @pytest.mark.unit
    def test_list_dags_with_status_filter(self, mock_responses):
        """Test DAG listing with status filter."""
        mock_responses.add(
            responses.GET,
            "http://localhost:8000/dags/list",
            json={"dags": [], "count": 0, "title": "Running DAGs"},
            status=200
        )
        
        result = self.client.list_dags(status_filter="running")
        
        # Check that status parameter was passed
        assert mock_responses.calls[0].request.url.endswith("?status=running")
    
    # Server Status Tests
    @pytest.mark.unit
    def test_is_server_running_true(self, mock_responses):
        """Test server running check returns True."""
        mock_responses.add(
            responses.GET,
            "http://localhost:8000/",
            json={"status": "healthy"},
            status=200
        )
        
        result = self.client.is_server_running()
        
        assert result is True
    
    @pytest.mark.unit
    def test_is_server_running_false(self, mock_responses):
        """Test server running check returns False."""
        mock_responses.add(
            responses.GET,
            "http://localhost:8000/",
            body=requests.exceptions.ConnectionError("Connection failed")
        )
        
        result = self.client.is_server_running()
        
        assert result is False
    
    @pytest.mark.unit
    def test_wait_for_server_success(self, mock_responses):
        """Test successful server wait."""
        mock_responses.add(
            responses.GET,
            "http://localhost:8000/",
            json={"status": "healthy"},
            status=200
        )
        
        result = self.client.wait_for_server(max_wait_time=1)
        
        assert result is True
    
    @pytest.mark.unit
    def test_wait_for_server_timeout(self, mock_responses):
        """Test server wait timeout."""
        mock_responses.add(
            responses.GET,
            "http://localhost:8000/",
            body=requests.exceptions.ConnectionError("Connection failed")
        )
        
        result = self.client.wait_for_server(max_wait_time=1)
        
        assert result is False
    
    # Error Handling Tests
    @pytest.mark.unit
    def test_make_request_timeout_error(self, mock_responses):
        """Test timeout error handling."""
        mock_responses.add(
            responses.GET,
            "http://localhost:8000/",
            body=requests.exceptions.Timeout("Request timed out")
        )
        
        with pytest.raises(TimeoutError) as exc_info:
            self.client.health_check()
        
        assert "Request to http://localhost:8000/ timed out" in str(exc_info.value)
    
    @pytest.mark.unit
    def test_make_request_http_error(self, mock_responses):
        """Test HTTP error handling."""
        mock_responses.add(
            responses.GET,
            "http://localhost:8000/",
            json={"error": "Server error"},
            status=500
        )
        
        with pytest.raises(RuntimeError) as exc_info:
            self.client.health_check()
        
        assert "HTTP 500" in str(exc_info.value)
    
    @pytest.mark.unit
    def test_make_request_custom_timeout(self):
        """Test client with custom timeout."""
        client = MaestroAPIClient(timeout=5)
        
        with patch.object(client.session, 'request') as mock_request:
            mock_request.return_value.json.return_value = {"status": "ok"}
            mock_request.return_value.raise_for_status.return_value = None
            
            client.health_check()
            
            # Verify that custom timeout was used
            mock_request.assert_called_once_with(
                "GET", "http://localhost:8000/", timeout=5
            )
