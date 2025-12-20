from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Optional, Type

import yaml
from pydantic import BaseModel, ValidationError

from maestro.server.internals.task_registry import TaskRegistry
from maestro.server.tasks.base import BaseTask
from maestro.shared.dag import DAG
from maestro.shared.task import Task, TaskStatus


class DAGConfig(BaseModel):
    """Schema for DAG configuration validation."""

    dag: Dict[str, Any]

    class Config:
        extra = "allow"


class DAGLoader:
    def __init__(self, task_registry: TaskRegistry):
        self.task_registry = task_registry

    def load_dag_from_file(self, filepath: str, dag_id: Optional[str] = None) -> DAG:
        """Load and validate DAG from YAML file."""
        filepath = str(Path(filepath).resolve())
        dag_id = dag_id or Path(filepath).stem

        try:
            with open(filepath, "r") as f:
                raw_config = yaml.safe_load(f)

            # Validate config structure
            config = DAGConfig(**raw_config)

        except (yaml.YAMLError, ValidationError) as e:
            raise ValueError(f"Invalid YAML configuration: {e}")
        except FileNotFoundError:
            raise FileNotFoundError(f"DAG file not found: {filepath}")

        # Extract scheduling configuration
        start_time = None
        cron_schedule = None

        if "start_time" in config.dag and "cron_schedule" in config.dag:
            raise ValueError("Cannot specify both start_time and cron_schedule")

        if "start_time" in config.dag:
            start_time_str = config.dag["start_time"]
            try:
                start_time = self._parse_datetime(start_time_str)
            except ValueError as e:
                raise ValueError(f"Invalid start_time format: {e}")

        if "cron_schedule" in config.dag:
            cron_schedule = config.dag["cron_schedule"]

        # Extract fail_fast execution policy (default: False)
        fail_fast = False
        if "fail_fast" in config.dag:
            if not isinstance(config.dag["fail_fast"], bool):
                raise ValueError("fail_fast must be a boolean (true/false)")
            fail_fast = config.dag["fail_fast"]

        dag = DAG(
            dag_id=dag_id,
            start_time=start_time,
            cron_schedule=cron_schedule,
            fail_fast=fail_fast,
        )

        dag_config = config.dag

        # Validate required fields
        if "tasks" not in dag_config:
            raise ValueError("DAG configuration must contain 'tasks' field")

        for task_config in dag_config["tasks"]:
            try:
                task = self._create_task_from_config(task_config, filepath)
                dag.add_task(task)
            except Exception as e:
                raise ValueError(
                    f"Error creating task '{task_config.get('task_id', 'unknown')}': {e}"
                )

        try:
            dag.validate()
        except Exception as e:
            raise ValueError(f"DAG validation failed: {e}")

        return dag

    def _create_task_from_config(
        self, task_config: Dict[str, Any], dag_file_path: str
    ) -> BaseTask:
        """Create a task instance from configuration."""
        task_config = task_config.copy()  # Don't modify original

        # Extract task type
        task_type_name = task_config.pop("type", None)
        if not task_type_name:
            raise ValueError("Task configuration must specify 'type'")

        task_class = self.task_registry.get(task_type_name)
        if not task_class:
            raise ValueError(f"Unknown task type: {task_type_name}")

        # Merge params + preserve condition

        params = task_config.pop("params", {})
        condition = task_config.get("condition")  # keep condition if present

        task_config.update(params)

        # Reinserisci condition se c'era
        if condition is not None:
            task_config["condition"] = condition

        # Add DAG file path
        task_config["dag_file_path"] = dag_file_path

        # Set default executor if not specified
        if "executor" not in task_config:
            task_config["executor"] = "local"

        try:
            return task_class(**task_config)
        except Exception as e:
            raise ValueError(f"Error instantiating {task_type_name}: {e}")

    def _parse_datetime(self, datetime_str: str) -> datetime:
        """Parse datetime string in various formats."""
        # Common datetime formats to try
        formats = [
            "%Y-%m-%d %H:%M:%S",  # 2023-12-25 14:30:00
            "%Y-%m-%dT%H:%M:%S",  # 2023-12-25T14:30:00 (ISO format)
            "%Y-%m-%dT%H:%M:%SZ",  # 2023-12-25T14:30:00Z (UTC)
            "%Y-%m-%d %H:%M",  # 2023-12-25 14:30
            "%Y-%m-%dT%H:%M",  # 2023-12-25T14:30
            "%Y-%m-%d",  # 2023-12-25 (assumes 00:00:00)
        ]

        for fmt in formats:
            try:
                return datetime.strptime(datetime_str, fmt)
            except ValueError:
                continue

        raise ValueError(
            f"Unable to parse datetime string '{datetime_str}'. Supported formats: {formats}"
        )

    def load_dag_from_dict(self, dag_dict: Dict[str, Any]) -> DAG:
        """Load a DAG from a dictionary representation."""
        dag_id = dag_dict.get("dag_id", "default_dag")
        start_time_str = dag_dict.get("start_time")
        start_time = self._parse_datetime(start_time_str) if start_time_str else None
        cron_schedule = dag_dict.get("cron_schedule")

        dag = DAG(dag_id=dag_id, start_time=start_time, cron_schedule=cron_schedule)

        # Get tasks from the dictionary
        tasks = dag_dict.get("tasks", {})

        # Handle tasks as a dictionary (from to_dict())
        for task_id, task_config in tasks.items():
            if isinstance(task_config, dict):
                # Add task_id to the config if not present
                if "task_id" not in task_config:
                    task_config["task_id"] = task_id

                # Ensure type field exists (from the serialized task model)
                # The type might be stored as the class name without 'Task' suffix
                if "type" not in task_config and task_config.get("task_id"):
                    # Try to infer type from other fields or use a default
                    if "playbook" in task_config:
                        task_config["type"] = "AnsibleTask"
                    elif (
                        "working_dir" in task_config and "workflow_mode" in task_config
                    ):
                        task_config["type"] = "ExtendedTerraformTask"
                    elif "message" in task_config:
                        task_config["type"] = "PrintTask"
                    elif "wait_seconds" in task_config:
                        task_config["type"] = "WaitTask"
                    elif "file_path" in task_config and "content" in task_config:
                        task_config["type"] = "FileWriterTask"
                    else:
                        # Default to PrintTask if we can't determine the type
                        task_config["type"] = "PrintTask"
                        if "message" not in task_config:
                            task_config["message"] = f"Task {task_id}"

                task = self._create_task_from_config(task_config, None)
                dag.add_task(task)

        dag.validate()
        return dag
