import yaml
from typing import Dict, Any
from rich import get_console

from maestro.core.dag import DAG
from maestro.core.task import Task, TaskStatus
from maestro.tasks.base import BaseTask
from maestro.tasks.print_task import PrintTask
from maestro.tasks.file_writer_task import FileWriterTask
from maestro.tasks.wait_task import WaitTask


class Orchestrator:
    def __init__(self):
        self.task_types: Dict[str, type[BaseTask]] = {
            "PrintTask": PrintTask,
            "FileWriterTask": FileWriterTask,
            "WaitTask": WaitTask,
        }

    def load_dag_from_file(self, filepath: str) -> DAG:
        with open(filepath, "r") as f:
            config = yaml.safe_load(f)

        dag = DAG()
        for task_config in config["dag"]["tasks"]:
            task_type_name = task_config.pop("type")
            task_type = self.task_types.get(task_type_name)
            if not task_type:
                raise ValueError(f"Unknown task type: {task_type_name}")

            params = task_config.pop("params", {})
            task_config.update(params) # Merge params into the main task_config

            task = task_type(**task_config)
            dag.add_task(task)

        dag.validate()
        return dag

    def run_dag(self, dag: DAG, status_manager=None, progress_tracker=None, status_callback=None):
        execution_order = dag.get_execution_order()
        console = get_console()

        for task_id in execution_order:
            task = dag.tasks[task_id]
            if status_manager:
                status_manager.set_task_status(task.task_id, "running")
            if status_callback:
                status_callback()
            try:
                task.status = TaskStatus.RUNNING
                console.print(f"Executing task: {task.task_id}")
                task.execute()
                task.status = TaskStatus.COMPLETED
                if status_manager:
                    status_manager.set_task_status(task.task_id, "completed")
                if progress_tracker:
                    progress_tracker.increment_completed()
                if status_callback:
                    status_callback()
                console.print(f"Task {task.task_id} completed.")
            except Exception as e:
                task.status = TaskStatus.FAILED
                if status_manager:
                    status_manager.set_task_status(task.task_id, "failed")
                if status_callback:
                    status_callback()
                console.print(f"Task {task.task_id} failed: {e}")
                break  # Stop execution on failure

    def visualize_dag(self, dag: DAG):
        # Basic ASCII visualization
        console = get_console()
        console.print("DAG Visualization:")
        for task_id, task in dag.tasks.items():
            console.print(f"- Task: {task_id} ({task.status.value})")
            if task.dependencies:
                console.print(f"  Dependencies: {', '.join(task.dependencies)}")

    def get_dag_status(self, dag: DAG) -> Dict[str, Any]:
        return {
            task_id: task.status.value for task_id, task in dag.tasks.items()
        }