"""Debug test to check server startup and database permissions"""
import subprocess
import time
import requests
import os
import tempfile
import shutil


def test_server_startup_manual():
    """Manually test server startup to debug permission issues"""
    server_port = 8765  # Use a different port
    server_url = f"http://127.0.0.1:{server_port}"
    
    # Create a temporary directory for the test database
    temp_dir = tempfile.mkdtemp()
    db_path = os.path.join(temp_dir, "test_maestro.db")
    
    print(f"Temp directory: {temp_dir}")
    print(f"Database path: {db_path}")
    
    try:
        # Start the server
        print(f"Starting Maestro server at {server_url}...")
        server_process = subprocess.Popen(
            ["maestro-server", "--port", str(server_port)],
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            env=os.environ.copy(),
            cwd=temp_dir
        )
        
        # Give it some time to start
        time.sleep(3)
        
        # Check if process is still running
        if server_process.poll() is not None:
            # Process exited, get output
            output, _ = server_process.communicate()
            print("Server exited prematurely!")
            print("Server output:")
            print(output)
            return
        
        # Try to connect
        try:
            response = requests.get(f"{server_url}/")
            print(f"Server responded with status code: {response.status_code}")
            print(f"Response: {response.json()}")
        except Exception as e:
            print(f"Failed to connect: {e}")
            
            # Get server output
            server_process.terminate()
            output, _ = server_process.communicate()
            print("Server output:")
            print(output)
            return
        
        # Check database file
        if os.path.exists(db_path):
            print(f"Database file created: {db_path}")
            print(f"Permissions: {oct(os.stat(db_path).st_mode)[-3:]}")
        else:
            # Check if database was created with default name
            default_db = os.path.join(temp_dir, "maestro.db")
            if os.path.exists(default_db):
                print(f"Database created at default location: {default_db}")
                print(f"Permissions: {oct(os.stat(default_db).st_mode)[-3:]}")
            else:
                print("No database file found!")
                print(f"Directory contents: {os.listdir(temp_dir)}")
        
        # Now test the CLI with server running
        env = os.environ.copy()
        env["MAESTRO_SERVER_URL"] = server_url
        
        print("\nTesting CLI list command...")
        result = subprocess.run(
            ["maestro", "list"],
            capture_output=True,
            text=True,
            env=env
        )
        print(f"CLI exit code: {result.returncode}")
        print(f"CLI stdout: {result.stdout}")
        print(f"CLI stderr: {result.stderr}")
        
        # Terminate server
        server_process.terminate()
        server_process.wait(timeout=5)
        
    finally:
        # Cleanup
        print(f"\nCleaning up {temp_dir}")
        shutil.rmtree(temp_dir, ignore_errors=True)


if __name__ == "__main__":
    test_server_startup_manual()
