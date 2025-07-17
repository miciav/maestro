from .base import BaseTask
import subprocess
import os
import shutil
from typing import Optional, Dict, Any
from pydantic import Field
from pathlib import Path


class TerraformTask(BaseTask):
    """
    A task for executing Terraform (or OpenTofu) commands.
    """

    # Define Pydantic fields
    command: Optional[str] = None
    working_dir: str  # This can be relative to the DAG file
    workspace: Optional[str] = None
    plan_file: str = "terraform.tfplan"
    auto_approve: bool = False
    backend_config: Dict[str, Any] = Field(default_factory=dict)
    vars: Dict[str, Any] = Field(default_factory=dict)

    def get_tf_command(self):
        """Determines whether to use 'tofu' or 'terraform'."""
        return self._find_tf_command()

    def get_absolute_working_dir(self):
        """
        Resolves the working directory to an absolute path.
        If working_dir is relative, it's resolved relative to the DAG file.
        """
        working_path = Path(self.working_dir)

        if working_path.is_absolute():
            return str(working_path)
        else:
            # Resolve relative to the DAG file directory
            if self.dag_file_path:
                dag_dir = Path(self.dag_file_path).parent
                absolute_path = dag_dir / working_path
                return str(absolute_path.resolve())
            else:
                # If no DAG file path is provided, resolve relative to current working directory
                return str(working_path.resolve())

    def _find_tf_command(self):
        """Determines whether to use 'tofu' or 'terraform'."""
        if shutil.which("tofu"):
            print("[TerraformTask] Using 'tofu' command.")
            return "tofu"
        elif shutil.which("terraform"):
            print("[TerraformTask] Using 'terraform' command.")
            return "terraform"
        else:
            raise FileNotFoundError("Neither 'tofu' nor 'terraform' command found in PATH.")

    def _build_command(self):
        """Builds the terraform command to be executed."""
        tf_cmd = self.get_tf_command()
        cmd = [tf_cmd]

        if self.workspace:
            cmd.extend(["workspace", "select", self.workspace])
            # This is a separate command, so we execute it first
            self.run_subprocess(cmd)
            # Now build the actual command
            cmd = [tf_cmd]

        if self.command:
            cmd.append(self.command)

        if self.command == "init":
            for key, value in self.backend_config.items():
                cmd.append(f"-backend-config={key}={value}")

        if self.command in ["plan", "apply"]:
            for key, value in self.vars.items():
                cmd.extend(["-var", f"{key}={value}"])

        if self.command == "plan":
            cmd.extend(["-out", self.plan_file])

        if self.command == "apply":
            if self.auto_approve:
                cmd.append("-auto-approve")
            cmd.append(self.plan_file)

        if self.command == "destroy":
            if self.auto_approve:
                cmd.append("-auto-approve")

        return cmd

    def run_subprocess(self, cmd):
        """Runs a subprocess command."""
        absolute_working_dir = self.get_absolute_working_dir()

        try:
            process = subprocess.run(
                cmd,
                cwd=absolute_working_dir,
                capture_output=True,
                text=True,
                check=True
            )
            print(process.stdout)
            if process.stderr:
                print(process.stderr)
        except subprocess.CalledProcessError as e:
            print(f"[TerraformTask] Error executing command: {' '.join(cmd)}")
            print(f"[TerraformTask] Working directory: {absolute_working_dir}")
            print(e.stdout)
            print(e.stderr)
            raise

    def execute_local(self):
        absolute_working_dir = self.get_absolute_working_dir()
        print(f"[TerraformTask] Executing '{self.task_id}'")
        print(f"[TerraformTask] Working directory: {absolute_working_dir}")
        cmd = self._build_command()
        self.run_subprocess(cmd)
        print(f"[TerraformTask] Task '{self.task_id}' completed successfully.")