
import pytest
import os
import time

# Assuming sample_dag.yaml exists in the examples directory
SAMPLE_DAG_PATH = os.path.join(os.path.dirname(__file__), "..", "..", "examples", "sample_dag.yaml")

def test_cli_run_dag_and_check_status(maestro_cli):
    """
    Tests running a DAG via CLI and then checking its status.
    """
    if not os.path.exists(SAMPLE_DAG_PATH):
        pytest.skip(f"Sample DAG not found at {SAMPLE_DAG_PATH}")

    # Run the sample DAG
    print(f"Attempting to run DAG from: {SAMPLE_DAG_PATH}")
    run_result = maestro_cli(["run", SAMPLE_DAG_PATH])
    assert "DAG 'sample_dag' started" in run_result.stdout
    assert run_result.returncode == 0

    # Extract DAG ID (assuming it's printed in the output)
    dag_id_line = next((line for line in run_result.stdout.splitlines() if "DAG ID:" in line), None)
    assert dag_id_line is not None, "DAG ID not found in run output"
    dag_id = dag_id_line.split("DAG ID:")[1].strip()
    print(f"Extracted DAG ID: {dag_id}")

    # Give some time for the DAG to process (adjust as needed)
    time.sleep(2)

    # Check the status of the DAG
    status_result = maestro_cli(["status", dag_id])
    assert f"Status for DAG ID: {dag_id}" in status_result.stdout
    assert "State: SUCCEEDED" in status_result.stdout or "State: RUNNING" in status_result.stdout
    assert status_result.returncode == 0

def test_cli_list_dags(maestro_cli):
    """
    Tests listing DAGs via CLI.
    """
    list_result = maestro_cli(["list"])
    assert "Listing all DAGs" in list_result.stdout
    assert list_result.returncode == 0

def test_cli_invalid_command(maestro_cli):
    """
    Tests an invalid CLI command.
    """
    invalid_result = maestro_cli(["nonexistent-command"], expected_exit_code=2)
    assert "Error: No such command 'nonexistent-command'" in invalid_result.stderr

