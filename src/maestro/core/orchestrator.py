import logging
from typing import Any, Type, Dict, Optional

from rich import get_console
from rich.logging import RichHandler

from maestro.core.dag import DAG
from maestro.core.task import TaskStatus
from maestro.tasks.base import BaseTask
from maestro.core.Task_Registry import TaskRegistry
from maestro.core.dag_loader import DAGLoader
from maestro.core.status_manager import StatusManager
from maestro.core.executors.factory import ExecutorFactory


class Orchestrator:
    def __init__(self, log_level: str = "INFO", db_path: str = "maestro.db"):
        self.task_registry = TaskRegistry()
        self.dag_loader = DAGLoader(self.task_registry)
        self.console = get_console()
        self.status_manager = StatusManager(db_path)
        self.executor_factory = ExecutorFactory()
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
        return self.dag_loader.load_dag_from_file(filepath)

    def run_dag(
        self, 
        dag: DAG, 
        resume: bool = False, 
        fail_fast: bool = True, 
        status_manager=None, 
        progress_tracker=None, 
        status_callback=None
    ):
        """Execute DAG with improved error handling and persistence."""
        dag_id = dag.dag_id
        execution_order = dag.get_execution_order()

        with self.status_manager as sm:
            if not resume:
                sm.reset_dag_status(dag_id)

            for task_id in execution_order:
                task = dag.tasks[task_id]
                task_status = sm.get_task_status(dag_id, task.task_id)

                if resume and task_status == "completed":
                    self.logger.info(f"Skipping already completed task: {task.task_id}")
                    task.status = TaskStatus.COMPLETED
                    if status_manager:
                        status_manager.set_task_status(task.task_id, "completed")
                    if progress_tracker:
                        progress_tracker.increment_completed()
                    if status_callback:
                        status_callback()
                    continue

                try:
                    task.status = TaskStatus.RUNNING
                    sm.set_task_status(dag_id, task.task_id, "running")
                    if status_manager:
                        status_manager.set_task_status(task.task_id, "running")
                    if status_callback:
                        status_callback()

                    self.logger.info(f"Executing task: {task.task_id}")
                    executor_instance = self.executor_factory.get_executor(task.executor)
                    task.execute(executor_instance)
                    task.status = TaskStatus.COMPLETED
                    sm.set_task_status(dag_id, task.task_id, "completed")
                    if status_manager:
                        status_manager.set_task_status(task.task_id, "completed")
                    if progress_tracker:
                        progress_tracker.increment_completed()
                    if status_callback:
                        status_callback()
                    self.logger.info(f"Task {task.task_id} completed successfully")

                except Exception as e:
                    task.status = TaskStatus.FAILED
                    sm.set_task_status(dag_id, task.task_id, "failed")
                    if status_manager:
                        status_manager.set_task_status(task.task_id, "failed")
                    if status_callback:
                        status_callback()

                    error_msg = f"Task {task.task_id} failed: {e}"
                    self.logger.error(error_msg)

                    if fail_fast:
                        raise Exception(error_msg)
                    else:
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