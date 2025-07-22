import logging
import uuid
import threading
from concurrent.futures import ThreadPoolExecutor, Future, as_completed
from typing import Any, Type, Dict, Optional, Set, List

from rich import get_console
from rich.logging import RichHandler

from maestro.shared.dag import DAG, DAGStatus
from maestro.shared.task import TaskStatus
from maestro.tasks.base import BaseTask
from maestro.server.internals.task_registry import TaskRegistry
from maestro.server.internals.dag_loader import DAGLoader
from maestro.server.internals.status_manager import StatusManager
from maestro.server.internals.executors.factory import ExecutorFactory


class DatabaseLogHandler(logging.Handler):
    """Custom logging handler that writes to StatusManager database."""

    def __init__(self, status_manager: StatusManager):
        super().__init__()
        self.status_manager = status_manager
        self.current_dag_id = None
        self.current_execution_id = None
        self.current_task_id = None

    def set_context(self, dag_id: str, execution_id: str, task_id: str = None):
        """Set the current execution context for logging."""
        self.current_dag_id = dag_id
        self.current_execution_id = execution_id
        self.current_task_id = task_id

    def emit(self, record):
        """Emit a log record to the database."""
        if self.current_dag_id and self.current_execution_id:
            try:
                log_entry = self.format(record)
                # Use the provided StatusManager instance
                with self.status_manager as sm:
                    sm.log_message(
                        dag_id=self.current_dag_id,
                        execution_id=self.current_execution_id,
                        task_id=self.current_task_id or "system",
                        level=record.levelname,
                        message=log_entry
                    )
            except Exception:
                # Don't let logging errors break the application
                pass


class Orchestrator:
    def __init__(self, log_level: str = "INFO", status_manager: Optional[StatusManager] = None, db_path: Optional[str] = "maestro.db"):
        self.task_registry = TaskRegistry()
        self.dag_loader = DAGLoader(self.task_registry)
        self.console = get_console()
        if status_manager:
            self.status_manager = status_manager
        elif db_path:
            self.status_manager = StatusManager(db_path)
        else:
            # This case should ideally not be hit if db_path has a default value.
            self.status_manager = StatusManager()
        self.executor_factory = ExecutorFactory()
        self._setup_logging(log_level)

        self.executor = ThreadPoolExecutor(max_workers=10)
        # Dictionary to track cancellation events for each execution
        self._execution_stop_events: Dict[str, threading.Event] = {}
        # Thread pool for concurrent task execution
        self.task_executor = ThreadPoolExecutor(max_workers=10)

    def _setup_logging(self, log_level: str):
        """Setup logging with Rich handler."""
        logging.basicConfig(
            level=getattr(logging, log_level.upper()),
            format="%(name)s - %(message)s",
            handlers=[RichHandler(console=self.console, rich_tracebacks=True)]
        )
        # Clear existing handlers from the root logger to prevent duplicate logs
        for handler in logging.root.handlers[:]:
            logging.root.removeHandler(handler)

        # Add RichHandler for console output
        rich_handler = RichHandler(console=self.console, rich_tracebacks=True)
        rich_handler.setLevel(getattr(logging, log_level.upper()))
        logging.root.addHandler(rich_handler)

        # Add DatabaseLogHandler for persistence
        self.db_handler = DatabaseLogHandler(self.status_manager)
        self.db_handler.setLevel(logging.DEBUG) # Capture all levels for database
        logging.root.addHandler(self.db_handler)

        # Set the root logger level
        logging.root.setLevel(getattr(logging, log_level.upper()))

        self.logger = logging.getLogger(__name__)

    

    def register_task_type(self, name: str, task_class: Type[BaseTask]):
        """Register a custom task type."""
        self.task_registry.register(name, task_class)
        self.logger.info(f"Registered task type: {name}")

    def load_dag_from_file(self, filepath: str, dag_id: Optional[str] = None) -> DAG:
        """Load and validate DAG from YAML file."""
        return self.dag_loader.load_dag_from_file(filepath, dag_id=dag_id)

    def run_dag_in_thread(
        self,
        dag: DAG,
        execution_id: str = None,
        resume: bool = False,
        fail_fast: bool = True,
        status_callback=None
    ) -> str:
        """Execute DAG in a separate thread with concurrency."""
        # Use provided execution_id or generate a new one
        if execution_id is None:
            execution_id = str(uuid.uuid4())
        
        dag.execution_id = execution_id
        
        # Create a stop event for this execution
        stop_event = threading.Event()
        self._execution_stop_events[execution_id] = stop_event
        
        # Create or update execution record in database synchronously
        with self.status_manager as sm:
            # Check if execution already exists (e.g., with 'created' status)
            existing = sm.get_latest_execution(dag.dag_id)
            if existing and existing["execution_id"] == execution_id:
                # Update existing execution to 'running'
                sm.update_dag_execution_status(dag.dag_id, execution_id, "running")
                # Tasks should already exist for this execution, don't reinitialize
            else:
                # Create new execution
                sm.create_dag_execution(dag.dag_id, execution_id)
                # Initialize all tasks with pending status only if not resuming
                if not resume:
                    task_ids = list(dag.tasks.keys())
                    sm.initialize_tasks_for_execution(dag.dag_id, execution_id, task_ids)

        # the body of the thread
        def execute():
            try:
                self.run_dag(
                    dag,
                    execution_id=execution_id,
                    resume=resume,
                    fail_fast=fail_fast,
                    status_callback=status_callback,
                    stop_event=stop_event
                )

                # After execution, check the final status of tasks
                with self.status_manager as sm:
                    final_statuses = sm.get_dag_status(dag.dag_id, execution_id)

                    # Count task statuses
                    has_failed = any(status == "failed" for status in final_statuses.values())
                    has_running = any(status == "running" for status in final_statuses.values())

                    # Determine overall DAG status
                    if has_running:
                        # Should not happen after execution completes, but handle it
                        dag.status = DAGStatus.RUNNING
                        sm.update_dag_execution_status(dag.dag_id, execution_id, "running")
                    elif has_failed:
                        dag.status = DAGStatus.FAILED
                        sm.update_dag_execution_status(dag.dag_id, execution_id, "failed")
                    else:
                        dag.status = DAGStatus.COMPLETED
                        sm.update_dag_execution_status(dag.dag_id, execution_id, "completed")

            except Exception as e:
                with self.status_manager as sm:
                    dag.status = DAGStatus.FAILED
                    # Mark any running or pending tasks as failed when DAG execution fails
                    sm.mark_incomplete_tasks_as_failed(dag.dag_id, execution_id)

                    # Update DAG execution status to failed
                    sm.update_dag_execution_status(dag.dag_id, execution_id, "failed")

                self.logger.error(f"DAG {dag.dag_id} execution failed: {e}")
                if fail_fast:
                    raise
            finally:
                # Clean up the stop event
                self._execution_stop_events.pop(execution_id, None)

        self.executor.submit(execute)
        return execution_id

    def run_dag(
        self,
        dag: DAG,
        execution_id: str = None,
        resume: bool = False,
        fail_fast: bool = True,
        status_manager=None,
        progress_tracker=None,
        status_callback=None,
        stop_event: Optional[threading.Event] = None
    ):
        """Execute DAG with concurrent task execution."""
        dag_id = dag.dag_id
        dag.status = DAGStatus.RUNNING

        # Set up database logging if execution_id is provided
        db_handler = None
        if execution_id:
            db_handler = DatabaseLogHandler(self.status_manager)
            db_handler.set_context(dag_id, execution_id)

            # Add the database handler to all task-related loggers
            task_loggers = [
                'maestro.tasks.terraform_task',
                'maestro.tasks.extended_terraform_task',
                'maestro.tasks.print_task',
                'maestro.core.executors.ssh',
                'maestro.core.executors.docker',
                'maestro.core.executors.local'
            ]

            for logger_name in task_loggers:
                logger = logging.getLogger(logger_name)
                logger.addHandler(db_handler)

        try:
            # Use concurrent execution
            self._run_dag_concurrent(
                dag=dag,
                execution_id=execution_id,
                resume=resume,
                fail_fast=fail_fast,
                status_callback=status_callback,
                progress_tracker=progress_tracker,
                stop_event=stop_event,
                db_handler=db_handler
            )
        finally:
            # Clean up database handler
            if db_handler:
                for logger_name in task_loggers:
                    logger = logging.getLogger(logger_name)
                    logger.removeHandler(db_handler)

    def _run_dag_concurrent(
        self,
        dag: DAG,
        execution_id: str,
        resume: bool,
        fail_fast: bool,
        status_callback,
        progress_tracker,
        stop_event: Optional[threading.Event],
        db_handler
    ):
        """Execute DAG tasks concurrently based on dependencies."""
        dag_id = dag.dag_id
        
        # Initialize task tracking sets
        pending_tasks: Set[str] = set(dag.tasks.keys())
        running_tasks: Dict[str, Future] = {}
        completed_tasks: Set[str] = set()
        failed_tasks: Set[str] = set()
        skipped_tasks: Set[str] = set()
        
        with self.status_manager as sm:
            # Handle resume - mark already completed tasks
            if resume:
                for task_id in list(pending_tasks):
                    task_status = sm.get_task_status(dag_id, task_id, execution_id)
                    if task_status == "completed":
                        self.logger.info(f"Task {task_id} already completed (resume mode)")
                        dag.tasks[task_id].status = TaskStatus.COMPLETED
                        completed_tasks.add(task_id)
                        pending_tasks.remove(task_id)
                        if progress_tracker:
                            progress_tracker.increment_completed()
                        if status_callback:
                            status_callback()
            
            # Main execution loop
            while pending_tasks or running_tasks:
                # Check for cancellation
                if stop_event and stop_event.is_set():
                    self.logger.info(f"DAG {dag_id} execution {execution_id} was cancelled")
                    self._handle_cancellation(
                        dag, execution_id, pending_tasks, running_tasks, sm, status_callback
                    )
                    break
                
                # Check for completed tasks
                if running_tasks:
                    completed_futures = []
                    for task_id, future in list(running_tasks.items()):
                        if future.done():
                            completed_futures.append((task_id, future))
                    
                    for task_id, future in completed_futures:
                        del running_tasks[task_id]
                        try:
                            future.result()  # This will raise any exceptions from the task
                            completed_tasks.add(task_id)
                            self.logger.info(f"Task {task_id} completed successfully")
                        except Exception as e:
                            failed_tasks.add(task_id)
                            self.logger.error(f"Task {task_id} failed: {e}")
                            if fail_fast:
                                # Cancel all running tasks and stop
                                self._cancel_running_tasks(running_tasks)
                                raise Exception(f"Task {task_id} failed: {e}")
                
                # Find tasks ready to run
                ready_tasks = self._find_ready_tasks(
                    dag, pending_tasks, completed_tasks, failed_tasks, skipped_tasks
                )
                
                # Submit ready tasks for execution
                for task_id in ready_tasks:
                    task = dag.tasks[task_id]
                    
                    # Check if dependencies failed
                    if self._has_failed_dependencies(task, failed_tasks, skipped_tasks):
                        self.logger.warning(f"Skipping task {task_id} because its dependencies failed")
                        task.status = TaskStatus.SKIPPED
                        sm.set_task_status(dag_id, task_id, "skipped", execution_id)
                        skipped_tasks.add(task_id)
                        pending_tasks.remove(task_id)
                        if status_callback:
                            status_callback()
                        continue
                    
                    # Submit task for execution
                    self.logger.info(f"Submitting task {task_id} for execution")
                    future = self.task_executor.submit(
                        self._execute_task_async,
                        task=task,
                        dag_id=dag_id,
                        execution_id=execution_id,
                        status_callback=status_callback,
                        progress_tracker=progress_tracker,
                        db_handler=db_handler
                    )
                    running_tasks[task_id] = future
                    pending_tasks.remove(task_id)
                
                # Brief sleep to prevent busy waiting
                if not ready_tasks and running_tasks:
                    threading.Event().wait(0.1)

    def _find_ready_tasks(
        self,
        dag: DAG,
        pending_tasks: Set[str],
        completed_tasks: Set[str],
        failed_tasks: Set[str],
        skipped_tasks: Set[str]
    ) -> List[str]:
        """Find tasks that are ready to run (all dependencies completed)."""
        ready_tasks = []
        
        for task_id in pending_tasks:
            task = dag.tasks[task_id]
            
            # Check if all dependencies are completed
            if all(dep in completed_tasks for dep in task.dependencies):
                ready_tasks.append(task_id)
        
        return ready_tasks

    def _has_failed_dependencies(
        self,
        task,
        failed_tasks: Set[str],
        skipped_tasks: Set[str]
    ) -> bool:
        """Check if task has any failed or skipped dependencies."""
        return any(
            dep in failed_tasks or dep in skipped_tasks
            for dep in task.dependencies
        )

    def _execute_task_async(
        self,
        task,
        dag_id: str,
        execution_id: str,
        status_callback,
        progress_tracker,
        db_handler
    ):
        """Execute a single task asynchronously."""
        task_id = task.task_id
        
        try:
            # Update status to running
            task.status = TaskStatus.RUNNING
            with self.status_manager as sm:
                sm.set_task_status(dag_id, task_id, "running", execution_id)
            
            if status_callback:
                status_callback()
            
            # Update database handler context for this task
            if db_handler:
                db_handler.set_context(dag_id, execution_id, task_id)
            
            # Execute the task
            self.logger.info(f"Executing task: {task_id}")
            executor_instance = self.executor_factory.get_executor(task.executor)
            task.execute(executor_instance)
            
            # Update status to completed
            task.status = TaskStatus.COMPLETED
            with self.status_manager as sm:
                sm.set_task_status(dag_id, task_id, "completed", execution_id)
            
            if progress_tracker:
                progress_tracker.increment_completed()
            if status_callback:
                status_callback()
                
        except Exception as e:
            # Update status to failed
            task.status = TaskStatus.FAILED
            with self.status_manager as sm:
                sm.set_task_status(dag_id, task_id, "failed", execution_id)
            
            if status_callback:
                status_callback()
            
            # Re-raise the exception to be caught by the main loop
            raise

    def _handle_cancellation(
        self,
        dag: DAG,
        execution_id: str,
        pending_tasks: Set[str],
        running_tasks: Dict[str, Future],
        sm,
        status_callback
    ):
        """Handle DAG cancellation."""
        # Cancel all running tasks
        self._cancel_running_tasks(running_tasks)
        
        # Mark DAG as cancelled
        sm.update_dag_execution_status(dag.dag_id, execution_id, "cancelled")
        
        # Mark pending tasks as skipped
        for task_id in pending_tasks:
            task = dag.tasks[task_id]
            task.status = TaskStatus.SKIPPED
            sm.set_task_status(dag.dag_id, task_id, "skipped", execution_id)
        
        if status_callback:
            status_callback()

    def _cancel_running_tasks(self, running_tasks: Dict[str, Future]):
        """Cancel all running task futures."""
        for task_id, future in running_tasks.items():
            if not future.done():
                future.cancel()
                self.logger.info(f"Cancelled running task: {task_id}")

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
            TaskStatus.FAILED: "red",
            TaskStatus.SKIPPED: "magenta"
        }
        return color_map.get(status, "white")

    def get_dag_status(self, dag: DAG) -> Dict[str, Any]:
        """Get comprehensive DAG status."""
        tasks_status = {}
        summary = {
            "pending": 0,
            "running": 0,
            "completed": 0,
            "failed": 0,
            "skipped": 0
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
    
    def cancel_dag_execution(self, dag_id: str, execution_id: str = None) -> bool:
        """Cancel a running DAG execution by setting its stop event."""
        with self.status_manager as sm:
            # If no execution_id provided, find the latest running execution
            if not execution_id:
                running_executions = sm.get_running_dags()
                for exec_info in running_executions:
                    if exec_info["dag_id"] == dag_id:
                        execution_id = exec_info["execution_id"]
                        break
            
            if execution_id and execution_id in self._execution_stop_events:
                # Set the stop event to signal the thread to stop
                self._execution_stop_events[execution_id].set()
                # Update the database status
                sm.cancel_dag_execution(dag_id, execution_id)
                return True
            
            return False
    
    def stop_all_running_dags(self) -> int:
        """Stop all currently running DAGs. Returns the number of DAGs stopped."""
        stopped_count = 0
        
        with self.status_manager as sm:
            running_dags = sm.get_running_dags()
            
            for dag_info in running_dags:
                dag_id = dag_info["dag_id"]
                execution_id = dag_info["execution_id"]
                
                if self.cancel_dag_execution(dag_id, execution_id):
                    stopped_count += 1
                    self.logger.info(f"Stopped DAG {dag_id} (execution: {execution_id})")
                else:
                    self.logger.warning(f"Failed to stop DAG {dag_id} (execution: {execution_id})")
        
        return stopped_count
