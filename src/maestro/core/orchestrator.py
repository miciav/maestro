import yaml
import logging
from typing import Dict, Any, Type, Optional
from pathlib import Path
from importlib import import_module
from pydantic import BaseModel, ValidationError

from rich import get_console
from rich.logging import RichHandler

from maestro.core.dag import DAG
from maestro.core.task import Task, TaskStatus
from maestro.tasks.base import BaseTask


class DAGConfig(BaseModel):
    """Schema for DAG configuration validation."""
    dag: Dict[str, Any]

    class Config:
        extra = "allow"


class TaskRegistry:
    """Registry for task types with plugin discovery."""

    def __init__(self):
        self._task_types: Dict[str, Type[BaseTask]] = {}
        self._load_builtin_tasks()

    def _load_builtin_tasks(self):
        """Load built-in task types."""
        from maestro.tasks.print_task import PrintTask
        from maestro.tasks.file_writer_task import FileWriterTask
        from maestro.tasks.wait_task import WaitTask
        from maestro.tasks.terraform_task import TerraformTask
        from maestro.tasks.ansible_task import AnsibleTask
        from maestro.tasks.extended_terraform_task import ExtendedTerraformTask

        self.register("PrintTask", PrintTask)
        self.register("FileWriterTask", FileWriterTask)
        self.register("WaitTask", WaitTask)
        self.register("TerraformTask", TerraformTask)
        self.register("AnsibleTask", AnsibleTask)
        self.register("ExtendedTerraformTask", ExtendedTerraformTask)

    def register(self, name: str, task_class: Type[BaseTask]):
        """Register a task type."""
        if not issubclass(task_class, BaseTask):
            raise ValueError(f"Task class {task_class} must inherit from BaseTask")
        self._task_types[name] = task_class

    def get(self, name: str) -> Optional[Type[BaseTask]]:
        """Get a task type by name."""
        return self._task_types.get(name)

    def list_types(self) -> Dict[str, Type[BaseTask]]:
        """List all registered task types."""
        return self._task_types.copy()


class Orchestrator:
    def __init__(self, log_level: str = "INFO"):
        self.task_registry = TaskRegistry()
        self.console = get_console()
        self._setup_logging(log_level)

    def _setup_logging(self, log_level: str):
        """Setup logging with Rich handler."""
        logging.basicConfig(
            level=getattr(logging, log_level.upper()),
            format="%(name)s - %(message)s",
            handlers=[RichHandler(console=self.console, rich_tracebacks=True)]
        )
        self.logger = logging.getLogger(__name__)

    def register_task_type(self, name: str, task_class: Type[BaseTask]):
        """Register a custom task type."""
        self.task_registry.register(name, task_class)
        self.logger.info(f"Registered task type: {name}")

    def load_dag_from_file(self, filepath: str) -> DAG:
        """Load and validate DAG from YAML file."""
        filepath = str(Path(filepath).resolve())

        try:
            with open(filepath, "r") as f:
                raw_config = yaml.safe_load(f)

            # Validate config structure
            config = DAGConfig(**raw_config)

        except (yaml.YAMLError, ValidationError) as e:
            raise ValueError(f"Invalid YAML configuration: {e}")
        except FileNotFoundError:
            raise FileNotFoundError(f"DAG file not found: {filepath}")

        dag = DAG()
        dag_config = config.dag

        # Validate required fields
        if "tasks" not in dag_config:
            raise ValueError("DAG configuration must contain 'tasks' field")

        for task_config in dag_config["tasks"]:
            try:
                task = self._create_task_from_config(task_config, filepath)
                dag.add_task(task)
            except Exception as e:
                raise ValueError(f"Error creating task '{task_config.get('task_id', 'unknown')}': {e}")

        try:
            dag.validate()
        except Exception as e:
            raise ValueError(f"DAG validation failed: {e}")

        return dag

    def _create_task_from_config(self, task_config: Dict[str, Any], dag_file_path: str) -> BaseTask:
        """Create a task instance from configuration."""
        task_config = task_config.copy()  # Don't modify original

        # Extract task type
        task_type_name = task_config.pop("type", None)
        if not task_type_name:
            raise ValueError("Task configuration must specify 'type'")

        task_class = self.task_registry.get(task_type_name)
        if not task_class:
            raise ValueError(f"Unknown task type: {task_type_name}")

        # Merge params into main config (for backward compatibility)
        params = task_config.pop("params", {})
        task_config.update(params)

        # Add DAG file path
        task_config["dag_file_path"] = dag_file_path

        try:
            return task_class(**task_config)
        except Exception as e:
            raise ValueError(f"Error instantiating {task_type_name}: {e}")

    def run_dag(self, dag: DAG, status_manager=None, progress_tracker=None,
                status_callback=None, fail_fast: bool = True):
        """Execute DAG with improved error handling."""
        execution_order = dag.get_execution_order()

        for task_id in execution_order:
            task = dag.tasks[task_id]

            try:
                # Update status to running
                task.status = TaskStatus.RUNNING
                if status_manager:
                    status_manager.set_task_status(task.task_id, "running")
                if status_callback:
                    status_callback()

                self.logger.info(f"Executing task: {task.task_id}")

                # Execute task
                task.execute()

                # Mark as completed
                task.status = TaskStatus.COMPLETED
                if status_manager:
                    status_manager.set_task_status(task.task_id, "completed")
                if progress_tracker:
                    progress_tracker.increment_completed()
                if status_callback:
                    status_callback()

                self.logger.info(f"Task {task.task_id} completed successfully")

            except Exception as e:
                # Mark as failed
                task.status = TaskStatus.FAILED
                if status_manager:
                    status_manager.set_task_status(task.task_id, "failed")
                if status_callback:
                    status_callback()

                error_msg = f"Task {task.task_id} failed: {e}"
                self.logger.error(error_msg)

                if fail_fast:
                    raise Exception(error_msg)
                else:
                    # Continue with remaining tasks if fail_fast is False
                    continue

    def visualize_dag(self, dag: DAG):
        """Visualize DAG structure."""
        self.console.print("DAG Visualization:", style="bold magenta")

        if not dag.tasks:
            self.console.print("No tasks in DAG", style="dim")
            return

        execution_order = dag.get_execution_order()

        for i, task_id in enumerate(execution_order, 1):
            task = dag.tasks[task_id]
            status_color = self._get_status_color(task.status)

            self.console.print(f"{i}. {task_id} ({task.status.value})", style=status_color)

            if task.dependencies:
                self.console.print(f"   Dependencies: {', '.join(task.dependencies)}", style="dim")

    def _get_status_color(self, status: TaskStatus) -> str:
        """Get color for task status."""
        color_map = {
            TaskStatus.PENDING: "blue",
            TaskStatus.RUNNING: "yellow",
            TaskStatus.COMPLETED: "green",
            TaskStatus.FAILED: "red"
        }
        return color_map.get(status, "white")

    def get_dag_status(self, dag: DAG) -> Dict[str, Any]:
        """Get comprehensive DAG status."""
        tasks_status = {}
        summary = {
            "pending": 0,
            "running": 0,
            "completed": 0,
            "failed": 0
        }

        for task_id, task in dag.tasks.items():
            status = task.status.value
            tasks_status[task_id] = {
                "status": status,
                "dependencies": task.dependencies,
                "type": task.__class__.__name__
            }
            summary[status] += 1

        return {
            "tasks": tasks_status,
            "summary": summary,
            "total_tasks": len(dag.tasks)
        }