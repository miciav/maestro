

import pytest
import subprocess
import time
import requests
import os

@pytest.fixture(scope="session")
def maestro_server():
    """
    Starts the Maestro REST server in a separate process and yields its URL.
    Ensures the server is shut down after tests.
    """
    server_url = "http://127.0.0.1:5000"
    server_process = None
    try:
        # Start the server
        print(f"Starting Maestro server at {server_url}...")
        server_process = subprocess.Popen(
            [os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "..", ".venv", "bin", "maestro-server")],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            env={**os.environ, "FLASK_ENV": "development"} # Ensure development mode for easier debugging
        )

        # Wait for the server to be ready
        max_retries = 20
        for i in range(max_retries):
            try:
                response = requests.get(f"{server_url}/health")
                if response.status_code == 200:
                    print("Maestro server is ready.")
                    break
            except requests.exceptions.ConnectionError:
                pass
            time.sleep(0.5)
        else:
            raise RuntimeError("Maestro server did not start within the expected time.")

        yield server_url

    finally:
        if server_process:
            print("Shutting down Maestro server...")
            server_process.terminate()
            server_process.wait(timeout=10)
            if server_process.poll() is None:
                server_process.kill()
            print("Maestro server shut down.")

@pytest.fixture
def maestro_cli(maestro_server):
    """
    Provides a callable to run maestro CLI commands, configured to connect
    to the test server.
    """
    def _run_cli_command(command_args, expected_exit_code=0):
        env = os.environ.copy()
        env["MAESTRO_SERVER_URL"] = maestro_server
        
        full_command = ["maestro"] + command_args
        print(f"Running CLI command: {' '.join(full_command)}")
        result = subprocess.run(
            full_command,
            capture_output=True,
            text=True,
            env=env,
            check=False # Do not raise exception for non-zero exit codes
        )
        print(f"CLI stdout:\n{result.stdout}")
        print(f"CLI stderr:\n{result.stderr}")
        assert result.returncode == expected_exit_code, (
            f"CLI command failed with exit code {result.returncode}. "
            f"Stdout: {result.stdout}, Stderr: {result.stderr}"
        )
        return result
    return _run_cli_command

