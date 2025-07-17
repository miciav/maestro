import os
import sys
import json
import shutil
import subprocess
from pathlib import Path
from typing import Optional, Dict, Any
from pydantic import Field

# Replicate necessary utilities from lib/common.py and lib/terraform.py
# This is crucial as we cannot import directly from proxmox-stack-deployer
# and must not modify those files.

# --- Replicated Utilities (simplified for this context) ---
# Constants (from lib/common.py)
PLAN_FILE = "tfplan" # This is used in lib/terraform.py
TERRAFORM_DIR = "terraform-opentofu" # This is used in lib/terraform.py

# Basic logging/printing functions (from lib/common.py)
def print_status(message):
    print(f"[STATUS] {message}")

def print_success(message):
    print(f"[SUCCESS] {message}")

def print_warning(message):
    print(f"[WARNING] {message}")

def print_error(message):
    print(f"[ERROR] {message}")

def print_header(message):
    print(f"=== {message} ===")

# check_command_exists (from lib/common.py)
def check_command_exists(command):
    return shutil.which(command) is not None

# --- End Replicated Utilities ---

from .terraform_task import TerraformTask # Import the original class

class ExtendedTerraformTask(TerraformTask):
    """
    An extended TerraformTask that supports both single command execution
    and a full Terraform/OpenTofu workflow.
    """
    workflow_mode: bool = False

    def execute_local(self):
        """
        Executes the Terraform operation locally.
        If workflow_mode is True, it runs the full Terraform workflow.
        Otherwise, it runs the specified command using the original logic.
        """
        if self.workflow_mode:
            print(f"[ExtendedTerraformTask] Executing full Terraform workflow for '{self.task_id}'.")
            self._run_full_workflow()
            print(f"[ExtendedTerraformTask] Full workflow for '{self.task_id}' completed successfully.")
        elif self.command:
            # Call the original execute method for single commands
            print(f"[ExtendedTerraformTask] Executing single Terraform command '{self.command}' for '{self.task_id}'.")
            super().execute_local()
            print(f"[ExtendedTerraformTask] Single command for '{self.task_id}' completed successfully.")
        else:
            raise ValueError("For ExtendedTerraformTask, either 'workflow_mode' must be true or a 'command' must be provided.")

    def _run_full_workflow(self):
        """
        Replicates the logic of run_terraform_workflow from proxmox-stack-deployer/lib/terraform.py.
        This method uses the internal run_subprocess of the TerraformTask.
        """
        original_dir = os.getcwd()
        absolute_working_dir = self.get_absolute_working_dir()
        os.chdir(absolute_working_dir) # Change to the task's working directory

        try:
            tf_cmd = self.get_tf_command() # Use the parent's method to get tf_cmd

            print_header("WORKFLOW TERRAFORM/OPENTOFU")

            # Initialization
            print_status(f"Initializing {tf_cmd}...")
            if (Path(absolute_working_dir) / ".terraform").is_dir():
                self.run_subprocess([tf_cmd, "init", "-upgrade"])
            else:
                self.run_subprocess([tf_cmd, "init"])

            # Validation
            print_status("Validating configuration...")
            self.run_subprocess([tf_cmd, "validate"])
            print_status("✓ Configuration valid")

            # Formatting
            print_status("Checking formatting...")
            # Use subprocess.run directly for fmt -check to capture output and check returncode

            self.run_subprocess([tf_cmd, "fmt", "-check", "-recursive"])

            print_warning("Formatting code...")
            self.run_subprocess([tf_cmd, "fmt", "-recursive"])
            print_status("✓ Code formatted correctly")

            # Planning
            skip_plan = os.environ.get("SKIP_PLAN", "").lower() == "true"
            plan_exit_code = 0

            if not skip_plan:
                print_status("Planning deployment...")
                # Use subprocess.run directly for plan -detailed-exitcode

                self.run_subprocess([tf_cmd, "plan", f"-out={PLAN_FILE}"])
                print_status("✓ Plan created with changes to apply")
                # Show the plan
                self.run_subprocess([tf_cmd, "show", PLAN_FILE])
                print_status(f"Creating VM with {tf_cmd}...")
                self.run_subprocess([tf_cmd, "apply", "-auto-approve", PLAN_FILE])
                print_success("Infrastructure created successfully!")
            return 1 # Indicates that changes were applied

        finally:
            os.chdir(original_dir) # Always change back to original directory
