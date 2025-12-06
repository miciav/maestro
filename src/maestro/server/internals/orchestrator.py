import logging
import re
import threading
import time
import uuid
from concurrent.futures import Future, ThreadPoolExecutor, as_completed
from datetime import datetime
from typing import Any, Dict, List, Optional, Set, Tuple, Type

from apscheduler.schedulers.background import BackgroundScheduler
from rich import get_console
from rich.logging import RichHandler

from maestro.server.internals.dag_loader import DAGLoader
from maestro.server.internals.executors.factory import ExecutorFactory
from maestro.server.internals.status_manager import StatusManager
from maestro.server.internals.task_registry import TaskRegistry
from maestro.server.tasks.base import BaseTask
from maestro.shared.dag import DAG, DAGStatus
from maestro.shared.task import TaskStatus


class DatabaseLogHandler(logging.Handler):
    """Thread-safe logging handler that writes task logs to StatusManager."""

    _ansi_escape = re.compile(r"\x1B\[[0-?]*[ -/]*[@-~]")

    def __init__(self, status_manager: StatusManager):
        super().__init__()
        self.status_manager = status_manager

        # Thread-local context instead of global instance attributes
        self._ctx = threading.local()

    # -------------------------------
    # CONTEXT HANDLING (THREAD-SAFE)
    # -------------------------------
    def set_context(self, dag_id: str, execution_id: str, task_id: str = None):
        """Assigns context to the current thread only."""
        self._ctx.dag_id = dag_id
        self._ctx.execution_id = execution_id
        self._ctx.task_id = task_id

    def clear_context(self):
        """Clears thread context after task finishes."""
        self._ctx.dag_id = None
        self._ctx.execution_id = None
        self._ctx.task_id = None

    # -------------------------------
    # LOG EMISSION
    # -------------------------------
    def emit(self, record):
        """Store logs in the database, per-thread context."""
        dag_id = getattr(self._ctx, "dag_id", None)
        execution_id = getattr(self._ctx, "execution_id", None)
        task_id = getattr(self._ctx, "task_id", None)

        # If we have no per-thread context: ignore this log
        if not dag_id or not execution_id:
            return

        try:
            # Extract real timestamp
            timestamp = datetime.fromtimestamp(record.created)

            # Raw message
            msg = record.getMessage()

            # Remove ANSI escape sequences (Rich formatting)
            msg = self._ansi_escape.sub("", msg)

            with self.status_manager as sm:
                sm.log_message(
                    dag_id=dag_id,
                    execution_id=execution_id,
                    task_id=task_id or "system",
                    level=record.levelname,
                    message=msg,
                    timestamp=timestamp,
                )

        except Exception:
            # Log errors should NEVER break execution
            pass


class Orchestrator:

    def __init__(
        self,
        log_level: str = "INFO",
        status_manager: Optional[StatusManager] = None,
        db_path: Optional[str] = "maestro.db",
    ):
        self.task_registry = TaskRegistry()
        self.dag_loader = DAGLoader(self.task_registry)
        self.console = get_console()

        if status_manager:
            self.status_manager = status_manager
        elif db_path:
            self.status_manager = StatusManager(db_path)
        else:
            self.status_manager = StatusManager()

        self.executor_factory = ExecutorFactory()

        # ⬇️ qui configuriamo i logger (vedi sotto)
        self._setup_logging(log_level)

        self.executor = ThreadPoolExecutor(max_workers=10)
        self._execution_stop_events: Dict[str, threading.Event] = {}
        self.task_executor = ThreadPoolExecutor(max_workers=10)

    def _setup_logging(self, log_level: str):
        """Setup global logging with RichHandler (console) and prepare DB handler."""

        logging.shutdown()
        for h in logging.root.handlers[:]:
            logging.root.removeHandler(h)

        level = getattr(logging, log_level.upper(), logging.INFO)
        logging.root.setLevel(level)

        # Console: solo RichHandler sul root
        rich_handler = RichHandler(console=self.console, rich_tracebacks=True)
        rich_handler.setLevel(level)

        root_logger = logging.getLogger()
        root_logger.addHandler(rich_handler)
        logging.captureWarnings(True)
        root_logger.propagate = False

        # Logger dell’orchestratore (non propaga al root)
        self.logger = logging.getLogger("maestro.orchestrator")
        self.logger.setLevel(level)
        self.logger.propagate = False

        # DB handler creato ma NON agganciato al root
        self.db_handler = DatabaseLogHandler(self.status_manager)
        self.db_handler.setLevel(logging.DEBUG)

    def register_task_type(self, name: str, task_class: Type[BaseTask]):
        """Register a custom task type."""
        self.task_registry.register(name, task_class)
        self.logger.info(f"Registered task type: {name}")

    def load_dag_from_file(self, filepath: str, dag_id: Optional[str] = None) -> DAG:
        """Load and validate DAG from YAML file."""
        return self.dag_loader.load_dag_from_file(filepath, dag_id=dag_id)

    def schedule_dag(self, dag: DAG):
        """Schedules a DAG to run based on its cron schedule."""
        if not dag.cron_schedule:
            self.logger.warning(
                f"DAG {dag.dag_id} has no cron schedule. Cannot schedule."
            )
            return

        job_id = f"dag:{dag.dag_id}"
        self.scheduler.add_job(
            self.execute_scheduled_dag,
            trigger="cron",
            id=job_id,
            name=dag.dag_id,
            args=[dag.dag_id],
            replace_existing=True,
            **dag.cron_schedule_to_aps_kwargs(),
        )
        self.logger.info(f"DAG {dag.dag_id} scheduled with cron: {dag.cron_schedule}")

    def unschedule_dag(self, dag_id: str):
        """Removes a DAG from the scheduler."""
        job_id = f"dag:{dag_id}"
        try:
            self.scheduler.remove_job(job_id)
            self.logger.info(f"DAG {dag_id} unscheduled.")
        except JobLookupError:
            self.logger.warning(f"Job for DAG {dag_id} not found in scheduler.")

    def execute_scheduled_dag(self, dag_id: str):
        """Function called by the scheduler to run a DAG."""
        self.logger.info(f"Scheduler triggered for DAG: {dag_id}")
        dag_filepath = self.status_manager.get_dag_filepath(dag_id)
        if not dag_filepath:
            self.logger.error(
                f"No file path found for scheduled DAG {dag_id}. Cannot execute."
            )
            return

        try:
            dag = self.load_dag_from_file(dag_filepath, dag_id=dag_id)
            self.run_dag_in_thread(dag, dag_filepath=dag_filepath)
        except Exception as e:
            self.logger.error(f"Failed to execute scheduled DAG {dag_id}: {e}")

    def run_dag_in_thread(
        self,
        dag: DAG,
        execution_id: str = None,
        resume: bool = False,
        fail_fast: bool = True,
        status_callback=None,
        dag_filepath: Optional[str] = None,  # Add this parameter
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
                sm.create_dag_execution(
                    dag.dag_id, execution_id, dag_filepath=dag_filepath
                )
                # Initialize all tasks with pending status only if not resuming
                if not resume:
                    task_ids = list(dag.tasks.keys())
                    sm.initialize_tasks_for_execution(
                        dag.dag_id, execution_id, task_ids
                    )

        # the body of the thread
        def execute():
            try:
                self.run_dag(
                    dag,
                    execution_id=execution_id,
                    resume=resume,
                    fail_fast=fail_fast,
                    status_callback=status_callback,
                    stop_event=stop_event,
                )

                # After execution, check the final status of tasks
                with self.status_manager as sm:
                    final_statuses = sm.get_dag_status(dag.dag_id, execution_id)

                    # Count task statuses
                    has_failed = any(
                        status == "failed" for status in final_statuses.values()
                    )
                    has_running = any(
                        status == "running" for status in final_statuses.values()
                    )

                    # Determine overall DAG status
                    if has_running:
                        # Should not happen after execution completes, but handle it
                        dag.status = DAGStatus.RUNNING
                        sm.update_dag_execution_status(
                            dag.dag_id, execution_id, "running"
                        )
                    elif has_failed:
                        dag.status = DAGStatus.FAILED
                        sm.update_dag_execution_status(
                            dag.dag_id, execution_id, "failed"
                        )
                    else:
                        dag.status = DAGStatus.COMPLETED
                        sm.update_dag_execution_status(
                            dag.dag_id, execution_id, "completed"
                        )

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
        stop_event: Optional[threading.Event] = None,
    ):
        """Execute DAG with concurrent task execution."""
        dag_id = dag.dag_id
        dag.status = DAGStatus.RUNNING

        # Set up database logging if execution_id is provided
        db_handler = None
        task_loggers = []

        if execution_id:
            db_handler = self.db_handler

            task_loggers = [
                "maestro.server.tasks.terraform_task",
                "maestro.server.tasks.extended_terraform_task",
                "maestro.server.tasks.print_task",
                "maestro.server.tasks.python_task",
                "maestro.server.tasks.bash_task",
                "maestro.core.executors.ssh",
                "maestro.core.executors.docker",
                "maestro.core.executors.local",
            ]

            for logger_name in task_loggers:
                logger = logging.getLogger(logger_name)

                # Pulisce eventuali handler duplicati
                logger.handlers.clear()

                # Aggancia DB handler
                logger.addHandler(db_handler)

                # Aggancia anche RichHandler (console) che sta sul root
                for h in logging.getLogger().handlers:
                    if isinstance(h, RichHandler):
                        logger.addHandler(h)

                logger.setLevel(logging.INFO)
                logger.propagate = False

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
                db_handler=db_handler,
            )

        finally:
            # Clean up database handler
            if db_handler:
                for logger_name in task_loggers:
                    logger = logging.getLogger(logger_name)
                    if db_handler in logger.handlers:
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
        db_handler,
    ):
        """
        Execute DAG tasks concurrently based on dependencies and dependency_policy.

        - Tasks are evaluated with evaluate_dependencies()
        - Tasks can be in states: run, wait, skip
        - Skipped tasks are updated in StatusManager
        """

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
                        self.logger.info(
                            f"Task {task_id} already completed (resume mode)"
                        )
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
                    self.logger.info(
                        f"DAG {dag_id} execution {execution_id} was cancelled"
                    )
                    self._handle_cancellation(
                        dag,
                        execution_id,
                        pending_tasks,
                        running_tasks,
                        sm,
                        status_callback,
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

                # Build upstream status dict
                upstream_statuses = {tid: "completed" for tid in completed_tasks}
                upstream_statuses.update({tid: "failed" for tid in failed_tasks})
                upstream_statuses.update({tid: "skipped" for tid in skipped_tasks})
                upstream_statuses.update({tid: "pending" for tid in pending_tasks})
                upstream_statuses.update({tid: "running" for tid in running_tasks})

                # Loop sui task pending
                for task_id in list(pending_tasks):
                    task = dag.tasks[task_id]
                    action = evaluate_dependencies(task, upstream_statuses)

                    if action == "run":
                        # Submit task for execution
                        self.logger.info(f"Submitting task {task_id} for execution")
                        future = self.task_executor.submit(
                            self._execute_task_async,
                            task=task,
                            dag_id=dag_id,
                            execution_id=execution_id,
                            status_callback=status_callback,
                            progress_tracker=progress_tracker,
                            db_handler=db_handler,
                        )
                        running_tasks[task_id] = future
                        pending_tasks.remove(task_id)

                    elif action == "wait":
                        continue  # lascia in pending

                    elif action == "skip":
                        task.status = TaskStatus.SKIPPED
                        sm.set_task_status(dag_id, task_id, "skipped", execution_id)
                        skipped_tasks.add(task_id)
                        pending_tasks.remove(task_id)
                        if status_callback:
                            status_callback()

                # Evita busy-waiting
                if pending_tasks or running_tasks:
                    threading.Event().wait(0.05)

    def _find_ready_tasks(
        self,
        dag: DAG,
        pending_tasks: Set[str],
        completed_tasks: Set[str],
        failed_tasks: Set[str],
        skipped_tasks: Set[str],
    ) -> List[str]:
        """
        Find tasks that are ready to run.

        Un task è "ready" quando:
        - tutte le sue dipendenze sono in stato terminale
        (completed, failed o skipped)
        - e, se ha una condition, questa viene valutata a True.

        I task la cui condition risulta False vengono marcati come SKIPPED qui.
        """
        ready_tasks: List[str] = []

        dag_id = dag.dag_id
        execution_id = dag.execution_id
        sm = StatusManager.get_instance()

        # Stati "terminali": la dipendenza non è più né pending né running
        terminal_deps = completed_tasks | failed_tasks | skipped_tasks

        # Task da rimuovere da pending perché skippati per condition False
        to_skip: List[str] = []

        for task_id in pending_tasks:
            task = dag.tasks[task_id]

            # Dipendenze ancora non "chiuse"? Non è pronto.
            if not all(dep in terminal_deps for dep in task.dependencies):
                continue

            # Se ha una condition, valutiamola.
            condition = getattr(task, "condition", None)
            if condition:
                if not self._evaluate_condition(task, dag_id, execution_id):
                    # Condition False → SKIPPED qui
                    task.status = TaskStatus.SKIPPED
                    skipped_tasks.add(task_id)
                    sm.set_task_status(dag_id, task_id, "skipped", execution_id)
                    to_skip.append(task_id)
                    self.logger.info(
                        f"Skipping task {task_id} due to condition: {condition}"
                    )
                    continue  # non è ready

            # Se siamo qui: tutte le dipendenze sono terminali
            # e la condition (se presente) è True → task pronto
            ready_tasks.append(task_id)

        # Rimuovi dai pending i task skippati per condition False
        for tid in to_skip:
            if tid in pending_tasks:
                pending_tasks.remove(tid)

        return ready_tasks

    def _evaluate_condition(self, task, dag_id: str, execution_id: str) -> bool:
        """Evaluate the boolean condition of a task using output_of()."""
        condition = getattr(task, "condition", None)
        if not condition:
            return True  # No condition → task is allowed

        sm = StatusManager.get_instance()

        # Helper to retrieve outputs from PythonTask
        def output_of(tid: str):
            result_json = sm.get_task_output(dag_id, tid, execution_id)
            return result_json  # Can be dict, list, int, str depending on task return

        try:
            # Safe environment
            safe_globals = {
                "__builtins__": {},
                "output_of": output_of,
            }
            safe_locals = {}

            return bool(eval(condition, safe_globals, safe_locals))

        except Exception as e:
            self.logger.error(
                f"Condition evaluation failed for task {task.task_id}: {e}"
            )
            return False

        print(
            "[DEBUG] condition eval:",
            task.task_id,
            expr,
            "output_of(check_condition)=",
            safe_evaluator_globals["output_of"]("check_condition"),
            flush=True,
        )

    def _execute_task_async(
        self,
        task,
        dag_id: str,
        execution_id: str,
        status_callback,
        progress_tracker,
        db_handler,
    ):
        """Execute a single task asynchronously WITH retry support."""

        task_id = task.task_id

        print(
            f"[DEBUG] task={task_id}, hasattr(retries)={hasattr(task, 'retries')}, "
            f"raw_retries={getattr(task, 'retries', None)!r}, dict={task.__dict__}",
            flush=True,
        )

        # Leggiamo i parametri di retry dal task, con fallback robusto
        raw_retries = getattr(task, "retries", 0)
        raw_retry_delay = getattr(task, "retry_delay", 0)

        try:
            max_retries = int(raw_retries or 0)
        except (TypeError, ValueError):
            max_retries = 0

        try:
            retry_delay = int(raw_retry_delay or 0)
        except (TypeError, ValueError):
            retry_delay = 0

        # Log di debug per capire cosa vede l’orchestratore
        self.logger.info(
            f"Task {task_id}: configured retries={max_retries}, retry_delay={retry_delay}"
        )

        attempt = 0

        while True:
            attempt += 1

            try:
                # Stato: running
                task.status = TaskStatus.RUNNING
                with self.status_manager as sm:
                    sm.set_task_status(dag_id, task_id, "running", execution_id)

                if status_callback:
                    status_callback()

                # Context per logging DB per questo thread/task
                if db_handler:
                    db_handler.set_context(dag_id, execution_id, task_id)

                # Passa info alla task (utile per PythonTask / BashTask)
                task.dag_id = dag_id
                task.execution_id = execution_id

                # Log del tentativo
                self.logger.info(
                    f"Executing task: {task_id} (attempt {attempt}/{max_retries + 1})"
                )

                executor_instance = self.executor_factory.get_executor(task.executor)
                task.execute(executor_instance)

                # Se arrivo qui: successo
                task.status = TaskStatus.COMPLETED
                with self.status_manager as sm:
                    sm.set_task_status(dag_id, task_id, "completed", execution_id)

                if progress_tracker:
                    progress_tracker.increment_completed()
                if status_callback:
                    status_callback()

                return  # task completata con successo → si esce

            except BaseException as e:
                # IGNORA SOLO I SEGNALI CHE DEVONO DAVVERO FERMARE TUTTO
                if isinstance(e, KeyboardInterrupt):
                    raise

                self.logger.error(f"Task {task_id} failed on attempt {attempt}: {e}")

                task.status = TaskStatus.FAILED
                with self.status_manager as sm:
                    sm.set_task_status(dag_id, task_id, "failed", execution_id)

                if attempt <= max_retries:
                    self.logger.info(
                        f"Retrying task {task_id} in {retry_delay} seconds "
                        f"({attempt}/{max_retries})"
                    )
                    if retry_delay > 0:
                        time.sleep(retry_delay)
                    continue
                else:
                    if status_callback:
                        status_callback()
                    raise Exception(f"Task {task_id} failed: {e}") from e

            finally:
                # Pulizia del contesto di logging
                if db_handler:
                    db_handler.clear_context()

    def _handle_cancellation(
        self,
        dag: DAG,
        execution_id: str,
        pending_tasks: Set[str],
        running_tasks: Dict[str, Future],
        sm,
        status_callback,
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

            self.console.print(
                f"{i}. {task_id} ({task.status.value})", style=status_color
            )

            if task.dependencies:
                self.console.print(
                    f"   Dependencies: {', '.join(task.dependencies)}", style="dim"
                )

    def _get_status_color(self, status: TaskStatus) -> str:
        """Get color for task status."""
        color_map = {
            TaskStatus.PENDING: "blue",
            TaskStatus.RUNNING: "yellow",
            TaskStatus.COMPLETED: "green",
            TaskStatus.FAILED: "red",
            TaskStatus.SKIPPED: "magenta",
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
            "skipped": 0,
        }

        for task_id, task in dag.tasks.items():
            status = task.status.value
            tasks_status[task_id] = {
                "status": status,
                "dependencies": task.dependencies,
                "type": task.__class__.__name__,
            }
            summary[status] += 1

        return {
            "tasks": tasks_status,
            "summary": summary,
            "total_tasks": len(dag.tasks),
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
                    self.logger.info(
                        f"Stopped DAG {dag_id} (execution: {execution_id})"
                    )
                else:
                    self.logger.warning(
                        f"Failed to stop DAG {dag_id} (execution: {execution_id})"
                    )

        return stopped_count


def evaluate_dependencies(task: BaseTask, upstream_statuses: Dict[str, str]) -> str:
    """
    Decide lo stato della task in base alle dipendenze.

    Parametri:
        task: il task da valutare
        upstream_statuses: dict {task_id: TaskStatus} dei task upstream

    Ritorna:
        "run" -> tutte le condizioni soddisfatte, task pronta a partire
        "wait" -> task deve attendere
        "skip" -> task deve essere skippata
    """
    policy = getattr(task, "dependency_policy", "none")
    dependencies = getattr(task, "dependencies", [])

    if not dependencies or policy == "none":
        return "run"

    statuses = [upstream_statuses.get(dep) for dep in dependencies]

    if policy == "all":
        if any(s is None or s in ("pending", "running") for s in statuses):
            return "wait"
        if all(s == "completed" for s in statuses):
            return "run"
        # Se una dipendenza ha fallito, skip task
        if any(s == "failed" for s in statuses):
            return "skip"

    elif policy == "any":
        if any(s == "completed" for s in statuses):
            return "run"
        if all(s in ("pending", "running", None) for s in statuses):
            return "wait"
        if all(s in ("failed", "skipped") for s in statuses):
            return "skip"

    return "run"
