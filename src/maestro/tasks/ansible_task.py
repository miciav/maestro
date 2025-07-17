from .base import BaseTask
import ansible_runner
import os
from typing import Optional, Dict, Any
from pydantic import Field
from pathlib import Path
from rich import get_console


class AnsibleTask(BaseTask):
    """
    A task for executing Ansible playbooks.
    """
    playbook: str
    inventory: str
    private_data_dir: str = "./"
    verbosity: int = 1
    extra_vars: Dict[str, Any] = Field(default_factory=dict)
    become_user: Optional[str] = None

    def get_absolute_paths(self):
        """Resolve paths relative to the DAG file if needed."""
        if self.dag_file_path:
            dag_dir = Path(self.dag_file_path).parent
            playbook_path = dag_dir / self.playbook if not Path(self.playbook).is_absolute() else Path(self.playbook)
            inventory_path = dag_dir / self.inventory if not Path(self.inventory).is_absolute() else Path(
                self.inventory)
            data_dir = dag_dir / self.private_data_dir if not Path(self.private_data_dir).is_absolute() else Path(
                self.private_data_dir)
        else:
            playbook_path = Path(self.playbook).resolve()
            inventory_path = Path(self.inventory).resolve()
            data_dir = Path(self.private_data_dir).resolve()

        return str(playbook_path), str(inventory_path), str(data_dir)

    def execute(self):
        console = get_console()
        console.print(f"[AnsibleTask] Executing '{self.task_id}'")

        playbook_path, inventory_path, data_dir = self.get_absolute_paths()

        # Validate required files exist
        if not os.path.exists(playbook_path):
            raise FileNotFoundError(f"Playbook not found: {playbook_path}")
        if not os.path.exists(inventory_path):
            raise FileNotFoundError(f"Inventory not found: {inventory_path}")

        console.print(f"[AnsibleTask] Using playbook: {playbook_path}")
        console.print(f"[AnsibleTask] Using inventory: {inventory_path}")

        result = ansible_runner.run(
            private_data_dir=data_dir,
            playbook=playbook_path,
            inventory=inventory_path,
            quiet=False,
            verbosity=self.verbosity
        )

        if result.status == "successful":
            console.print(f"[AnsibleTask] Task '{self.task_id}' completed successfully.")
        else:
            error_msg = f"[AnsibleTask] Task '{self.task_id}' failed with status: {result.status}"
            console.print(error_msg, style="red")
            raise Exception(error_msg)