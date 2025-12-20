#!/usr/bin/env python3

import json
import time
import urllib.parse
from datetime import datetime
from typing import Any, Dict, Iterator, List, Optional

import requests


class MaestroAPIClient:
    """REST client for communicating with Maestro API server"""

    def __init__(self, base_url: str = "http://localhost:8000", timeout: int = 30):
        self.base_url = base_url.rstrip("/")
        self.timeout = timeout
        self.session = requests.Session()

    def _make_request(self, method: str, endpoint: str, **kwargs) -> requests.Response:
        """Make HTTP request with error handling"""
        url = f"{self.base_url}{endpoint}"

        try:
            response = self.session.request(method, url, timeout=self.timeout, **kwargs)
            response.raise_for_status()
            return response
        except requests.exceptions.ConnectionError:
            raise ConnectionError(
                f"Could not connect to Maestro server at {self.base_url}"
            )
        except requests.exceptions.Timeout:
            raise TimeoutError(f"Request to {url} timed out")
        except requests.exceptions.HTTPError as e:
            if response.status_code == 404:
                raise FileNotFoundError(f"Resource not found: {url}")
            elif response.status_code == 400:
                error_detail = response.json().get("detail", str(e))
                raise ValueError(f"Bad request: {error_detail}")
            else:
                raise RuntimeError(f"HTTP {response.status_code}: {response.text}")

    def health_check(self) -> Dict[str, Any]:
        """Check if the server is running"""
        response = self._make_request("GET", "/")
        return response.json()

    def create_dag(
        self, dag_file_path: str, dag_id: Optional[str] = None
    ) -> Dict[str, Any]:
        """Create a new DAG from a YAML file"""
        data = {"dag_file_path": dag_file_path}
        if dag_id:
            data["dag_id"] = dag_id
        response = self._make_request(
            method="POST", endpoint="/v1/dags/create", json=data
        )
        return response.json()

    def run_dag(
        self, dag_id: str, resume: bool = False, fail_fast: Optional[bool] = None
    ) -> Dict[str, Any]:
        """Run a previously created DAG"""
        data = {"resume": resume}
        if fail_fast is not None:
            data["fail_fast"] = fail_fast
        response = self._make_request(
            method="POST", endpoint=f"/v1/dags/{dag_id}/run", json=data
        )
        return response.json()

    def get_dag_status(
        self, dag_id: str, execution_id: Optional[str] = None
    ) -> Dict[str, Any]:
        """Get status of a specific DAG execution"""
        endpoint = f"/v1/dags/{dag_id}/status"
        params = {}
        if execution_id:
            params["execution_id"] = execution_id

        response = self._make_request("GET", endpoint, params=params)
        return response.json()

    def get_dag_logs_v1(
        self,
        dag_id: str,
        execution_id: Optional[str] = None,
        limit: int = 100,
        task_filter: Optional[str] = None,
        level_filter: Optional[str] = None,
    ) -> List[Dict[str, Any]]:
        """Get logs for a specific DAG execution using the v1 API"""
        endpoint = f"/v1/logs/{dag_id}"
        params = {"limit": limit}

        if execution_id:
            params["execution_id"] = execution_id
        if task_filter:
            params["task_filter"] = task_filter
        if level_filter:
            params["level_filter"] = level_filter

        response = self._make_request("GET", endpoint, params=params)
        return response.json()

    # TODO: refactor, this is used only for testing
    def stream_dag_logs_v1(
        self,
        dag_id: str,
        execution_id: Optional[str] = None,
        task_filter: Optional[str] = None,
        level_filter: Optional[str] = None,
    ) -> Iterator[Dict[str, Any]]:
        """
        Stream logs for a specific DAG execution in real-time using the v1 API.
        FIX: punta all'endpoint /v1/logs/{dag_id}/attach esposto dal server.
        """

        # 🚩 QUI il vero fix: usare "logs" e non "dags", e mantenere il prefisso /v1
        endpoint = f"/v1/logs/{dag_id}/attach"

        params = {}
        if execution_id:
            params["execution_id"] = execution_id
        if task_filter:
            params["task_filter"] = task_filter
        if level_filter:
            params["level_filter"] = level_filter

        url = f"{self.base_url}{endpoint}"
        if params:
            url += "?" + urllib.parse.urlencode(params)

        try:
            with self.session.get(url, stream=True, timeout=None) as response:
                response.raise_for_status()

                for line in response.iter_lines():
                    if not line:
                        continue

                    line = line.decode("utf-8").strip()
                    if line.startswith("data: "):
                        data = line[len("data: ") :]
                        try:
                            yield json.loads(data)
                        except json.JSONDecodeError:
                            continue

        except requests.exceptions.ConnectionError:
            raise ConnectionError(
                f"Could not connect to Maestro server at {self.base_url}"
            )
        except requests.exceptions.HTTPError as e:
            raise RuntimeError(f"HTTP {response.status_code}: {response.text}")

    def get_running_dags(self) -> Dict[str, Any]:
        """Get all currently running DAGs"""
        response = self._make_request("GET", "/dags/running")
        return response.json()

    def cancel_dag(
        self, dag_id: str, execution_id: Optional[str] = None
    ) -> Dict[str, Any]:
        """Cancel a running DAG execution"""
        endpoint = f"/dags/{dag_id}/cancel"
        params = {}
        if execution_id:
            params["execution_id"] = execution_id

        response = self._make_request("POST", endpoint, params=params)
        return response.json()

    def validate_dag(self, dag_file_path: str) -> Dict[str, Any]:
        """Validate a DAG file without executing it"""
        endpoint = f"/dags/validate"
        data = {"dag_file_path": dag_file_path}

        response = self._make_request("POST", endpoint, json=data)
        return response.json()

    def cleanup_old_executions(self, days: int = 30) -> Dict[str, Any]:
        """Clean up old execution records"""
        endpoint = "/dags/cleanup"
        params = {"days": days}

        response = self._make_request("DELETE", endpoint, params=params)
        return response.json()

    def remove_dag(self, dag_id: str, force: bool = False) -> Dict[str, Any]:
        """Remove a DAG and its executions"""
        endpoint = f"/v1/dags/{dag_id}"
        params = {"force": force}
        response = self._make_request("DELETE", endpoint, params=params)
        return response.json()

    # TODO: refactor. This function is used only for test
    def list_dags(self, status_filter: Optional[str] = None) -> Dict[str, Any]:
        """List all DAGs with optional status filtering (legacy)"""
        # This method is now a wrapper around list_dags_v1
        # It's kept for backward compatibility with the old CLI 'list' command

        if status_filter == "running":
            active_dag_lst = self.list_dags_v1("active")
            return {
                "dags": active_dag_lst,
                "title": "Running DAGs",
                "count": len(active_dag_lst),
            }
        else:
            all_dag_lst = self.list_dags_v1("all")
            return {"dags": all_dag_lst, "title": "All DAGs", "count": len(all_dag_lst)}

    def list_dags_v1(self, filter: Optional[str] = None) -> List[Dict[str, Any]]:
        """List all DAGs with optional filtering using the v1 API"""
        endpoint = "/v1/dags"
        params = {}
        if filter:
            params["status"] = filter

        response = self._make_request("GET", endpoint, params=params)
        return response.json()

    def stop_dag(
        self, dag_id: str, execution_id: Optional[str] = None
    ) -> Dict[str, Any]:
        """Stop a running DAG execution"""
        endpoint = f"/v1/dags/{dag_id}/stop"
        data = {}
        if execution_id:
            data["execution_id"] = execution_id
        response = self._make_request("POST", endpoint, json=data)
        return response.json()

    def resume_dag(self, dag_id: str, execution_id: str) -> Dict[str, Any]:
        """Resume a previously stopped DAG execution"""
        endpoint = f"/v1/dags/{dag_id}/resume"
        data = {"dag_id": dag_id, "execution_id": execution_id}
        response = self._make_request("POST", endpoint, json=data)
        return response.json()

    def is_server_running(self) -> bool:
        """Check if the Maestro server is running"""
        try:
            self.health_check()
            return True
        except (ConnectionError, TimeoutError):
            return False

    def wait_for_server(self, max_wait_time: int = 30) -> bool:
        """Wait for the server to become available"""
        start_time = time.time()
        while time.time() - start_time < max_wait_time:
            if self.is_server_running():
                return True
            time.sleep(1)
        return False
