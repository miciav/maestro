import yaml
from typing import Dict, Any, Type, Optional
from pathlib import Path
from pydantic import BaseModel, ValidationError

from maestro.core.dag import DAG
from maestro.core.task import Task, TaskStatus
from maestro.tasks.base import BaseTask
from maestro.core.Task_Registry import TaskRegistry # Assuming this is the correct import for TaskRegistry

class DAGConfig(BaseModel):
    """Schema for DAG configuration validation."""
    dag: Dict[str, Any]

    class Config:
        extra = "allow"

class DAGLoader:
    def __init__(self, task_registry: TaskRegistry):
        self.task_registry = task_registry

    def load_dag_from_file(self, filepath: str) -> DAG:
        """Load and validate DAG from YAML file."""
        filepath = str(Path(filepath).resolve())
        dag_id = Path(filepath).stem

        try:
            with open(filepath, "r") as f:
                raw_config = yaml.safe_load(f)

            # Validate config structure
            config = DAGConfig(**raw_config)

        except (yaml.YAMLError, ValidationError) as e:
            raise ValueError(f"Invalid YAML configuration: {e}")
        except FileNotFoundError:
            raise FileNotFoundError(f"DAG file not found: {filepath}")

        dag = DAG(dag_id=dag_id)
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
