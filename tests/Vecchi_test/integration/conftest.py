

import pytest
import subprocess
import time
import requests
import os
import tempfile

@pytest.fixture(scope="session")
def maestro_server():
    """
    Starts the Maestro REST server in a separate process and yields its URL.
    Ensures the server is shut down after tests.
    """
    server_port = 8765  # Use a different port to avoid conflicts
    server_url = f"http://127.0.0.1:{server_port}"
    server_process = None
    
    # Create a temporary directory for the test database
    temp_dir = tempfile.mkdtemp()
    db_path = os.path.join(temp_dir, "test_maestro.db")
    
    try:
        # Start the server from the temporary directory to ensure write permissions
        print(f"Starting Maestro server at {server_url}...")
        print(f"Using database: {db_path}")
        
        env = os.environ.copy()
        
        server_process = subprocess.Popen(
            [os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "..", ".venv", "bin", "maestro-server"), "--port", str(server_port)],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            env=env,
            cwd=temp_dir  # Change working directory to temp dir
        )

        # Wait for the server to be ready
        max_retries = 30
        for i in range(max_retries):
            try:
                response = requests.get(f"{server_url}/")  # Use root endpoint for health check
                if response.status_code == 200:
                    print("Maestro server is ready.")
                    break
            except requests.exceptions.ConnectionError:
                pass
            time.sleep(1)
        else:
            # Check if process exited with error
            if server_process.poll() is not None:
                stdout, stderr = server_process.communicate()
                print(f"Server stdout: {stdout}")
                print(f"Server stderr: {stderr}")
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
        
        # Clean up temporary directory
        import shutil
        if 'temp_dir' in locals():
            shutil.rmtree(temp_dir, ignore_errors=True)

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

