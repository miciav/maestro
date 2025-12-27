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

    def set_context(
        self,
        dag_id: str,
        execution_run_name: str,
        task_id: str | None = None,
        attempt_number: int | None = None,
        status: str | None = None,
    ):
        # Salva tutto nel thread-local context
        self._ctx.dag_id = dag_id
        self._ctx.execution_id = execution_run_name
        self._ctx.task_id = task_id
        self._ctx.attempt_number = attempt_number
        self._ctx.status = status

    def clear_context(self):
        """Clears thread-local context after task finishes."""
        self._ctx.dag_id = None
        self._ctx.execution_id = None
        self._ctx.task_id = None
        self._ctx.attempt_number = None
        self._ctx.status = None

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
        self.logger.propagate = True

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

            print(
                f"[DEBUG DAG] dag_id={dag.dag_id} fail_fast={dag.fail_fast}", flush=True
            )

            print(
                f"[DEBUG DAG] explicit_exit_tasks={dag.get_explicit_exit_tasks()}",
                flush=True,
            )

            self.run_dag_in_thread(dag, dag_filepath=dag_filepath)
        except Exception as e:
            self.logger.error(f"Failed to execute scheduled DAG {dag_id}: {e}")

    def run_dag_in_thread(
        self,
        dag: DAG,
        execution_id: str = None,
        resume: bool = False,
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

            # 1️⃣ crea execution SE non esiste
            existing = sm.get_latest_execution(dag.dag_id)

            if not existing or existing["execution_id"] != execution_id:
                sm.create_dag_execution(
                    dag.dag_id,
                    execution_id,
                    dag_filepath=dag_filepath,
                    fail_fast=dag.fail_fast,
                )

                if not resume:
                    sm.initialize_tasks_for_execution(
                        dag.dag_id,
                        execution_id,
                        dag.tasks,
                    )

            # 2️⃣ TRANSIZIONE OBBLIGATORIA A RUNNING
            sm.update_dag_execution_status(
                dag.dag_id,
                execution_id,
                "running",
            )

        # the body of the thread
        def execute():
            try:

                self.logger.warning(
                    "[DAG DEBUG] "
                    f"dag_id={dag.dag_id} "
                    f"fail_fast={dag.fail_fast} "
                    f"tasks={[t.task_id for t in dag.tasks.values()]}"
                )

                for t in dag.tasks.values():
                    pol = getattr(t, "dependency_policy", None)
                    self.logger.warning(
                        f"[DAG DEBUG] task={t.task_id} "
                        f"deps={t.dependencies} "
                        f"policy={pol} "
                        f"policy_type={type(pol)} "
                        f"is_final={getattr(t, 'is_final', False)}"
                    )

                self.logger.warning(
                    f"[FAIL_FAST RESOLVE] dag_id={dag.dag_id} execution_id={execution_id} dag.fail_fast={getattr(dag, 'fail_fast', None)}"
                )

                self.run_dag(
                    dag,
                    execution_id=execution_id,
                    resume=resume,
                    status_callback=status_callback,
                    stop_event=stop_event,
                )

                with self.status_manager as sm:

                    sm.update_execution_status_from_final_task(
                        dag.dag_id,
                        execution_id,
                    )

            except Exception as e:
                with self.status_manager as sm:
                    dag.status = DAGStatus.FAILED
                    # Mark any running or pending tasks as failed when DAG execution fails
                    sm.mark_incomplete_tasks_as_failed(dag.dag_id, execution_id)

                    # Update DAG execution status to failed
                    sm.update_dag_execution_status(dag.dag_id, execution_id, "failed")

                self.logger.error(f"DAG {dag.dag_id} execution failed: {e}")
                if dag.fail_fast:
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

        self.logger.warning(
            f"[DAG FAIL_FAST] dag_id={dag_id} execution_id={execution_id} dag.fail_fast={getattr(dag, 'fail_fast', None)}"
        )

        # Initialize task tracking sets
        pending_tasks: Set[str] = set(dag.tasks.keys())
        running_tasks: Dict[str, Future] = {}
        completed_tasks: Set[str] = set()
        failed_tasks: Set[str] = set()
        skipped_tasks: Set[str] = set()

        # 🆕 FAIL-FAST SOFT STOP FLAG
        stop_scheduling = False

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

            no_progress_ticks = 0
            last_snapshot = None

            # Main execution loop
            while pending_tasks or running_tasks:

                snapshot = (
                    tuple(sorted(pending_tasks)),
                    tuple(sorted(running_tasks.keys())),
                    tuple(sorted(completed_tasks)),
                    tuple(sorted(failed_tasks)),
                    tuple(sorted(skipped_tasks)),
                    stop_scheduling,
                )

                if snapshot == last_snapshot and not running_tasks and pending_tasks:
                    no_progress_ticks += 1
                else:
                    no_progress_ticks = 0
                last_snapshot = snapshot

                # dopo ~3 secondi (60 * 0.05) senza progresso: deadlock
                if no_progress_ticks >= 60:
                    self.logger.error(
                        f"[DEADLOCK] No progress for execution {execution_id}. "
                        f"Pending tasks cannot be scheduled: {sorted(pending_tasks)}"
                    )
                    # scegli una policy: io direi "failed" per chiarezza
                    for tid in list(pending_tasks):
                        sm.set_task_status(dag_id, tid, "failed", execution_id)
                        failed_tasks.add(tid)
                        pending_tasks.remove(tid)
                    break

                self.logger.warning(
                    "[LOOP SNAPSHOT] "
                    f"pending={list(pending_tasks)} "
                    f"running={list(running_tasks.keys())} "
                    f"completed={list(completed_tasks)} "
                    f"failed={list(failed_tasks)} "
                    f"skipped={list(skipped_tasks)} "
                    f"stop_scheduling={stop_scheduling}"
                )

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
                            future.result()
                            completed_tasks.add(task_id)
                            self.logger.info(f"Task {task_id} completed successfully")

                        except Exception as e:
                            failed_tasks.add(task_id)
                            self.logger.error(f"Task {task_id} failed: {e}")

                            if dag.fail_fast:
                                stop_scheduling = True

                                self.logger.warning(
                                    f"[DAG FAIL_FAST] dag_id={dag_id} execution_id={execution_id} dag.fail_fast={getattr(dag, 'fail_fast', None)}"
                                )

                # Build upstream status dict (simple string statuses)
                upstream_statuses = {tid: "completed" for tid in completed_tasks}
                upstream_statuses.update({tid: "failed" for tid in failed_tasks})
                upstream_statuses.update({tid: "skipped" for tid in skipped_tasks})
                upstream_statuses.update({tid: "pending" for tid in pending_tasks})
                upstream_statuses.update({tid: "running" for tid in running_tasks})

                # Loop sui task pending
                for task_id in list(pending_tasks):

                    task = dag.tasks[task_id]
                    is_final = bool(getattr(task, "is_final", False))

                    pol = getattr(task, "dependency_policy", None)

                    self.logger.warning(
                        f"[TASK EVAL] task={task_id} "
                        f"policy={pol} "
                        f"is_final={is_final} "
                        f"stop_scheduling={stop_scheduling}"
                    )

                    # ================================
                    # ### PATCH 1 — FAIL-FAST HANDLING — SOFT STOP ASSOLUTO
                    # ================================

                    if stop_scheduling and dag.fail_fast:
                        task.status = TaskStatus.SKIPPED

                        sm.set_task_status(dag_id, task_id, "skipped", execution_id)

                        skipped_tasks.add(task_id)
                        pending_tasks.remove(task_id)

                        if status_callback:
                            status_callback()

                        self.logger.warning(
                            f"[FAIL-FAST] Skipping task {task_id} (soft stop)"
                        )
                        continue

                    # ================================
                    # ### PATCH 2 — NORMALE VALUTAZIONE
                    # ================================
                    action = evaluate_dependencies(
                        task, upstream_statuses, sm, dag_id, execution_id
                    )

                    self.logger.warning(
                        f"[TASK DECISION] task={task_id} action={action}"
                    )

                    if action == "run":
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
                        continue

                    elif action == "skip":
                        task.status = TaskStatus.SKIPPED

                        sm.set_task_status(dag_id, task_id, "skipped", execution_id)

                        skipped_tasks.add(task_id)
                        pending_tasks.remove(task_id)

                        if status_callback:
                            status_callback()

                        self.logger.info(f"Task {task_id} skipped (policy/condition)")

                # Evita busy-waiting: aspetta un breve intervallo se ci sono task attivi
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

                    sm.set_task_status(
                        dag_id,
                        task_id,
                        "skipped",
                        execution_id,
                    )

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

        # --- Retry config ---
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

        self.logger.info(
            f"Task {task_id}: configured retries={max_retries}, retry_delay={retry_delay}"
        )

        attempt = 0

        while True:
            attempt += 1
            attempt_id = None

            # --- create attempt (best-effort) ---
            try:
                with self.status_manager as sm:
                    attempt_id = sm.create_task_attempt(
                        dag_id=dag_id,
                        task_id=task_id,
                        execution_id=execution_id,
                        attempt_number=attempt,
                    )
            except Exception as ex:
                self.logger.warning(
                    f"[task_attempts] create_task_attempt failed for {task_id}: {ex}"
                )
                attempt_id = None

            try:
                # Stato task: running
                task.status = TaskStatus.RUNNING
                with self.status_manager as sm:
                    sm.set_task_status(dag_id, task_id, "running", execution_id)

                if status_callback:
                    status_callback()

                if db_handler:
                    try:
                        db_handler.set_context(
                            dag_id=dag_id,
                            execution_run_name=execution_id,  # (anche se è ancora UUID, intanto non deve crashare)
                            task_id=task_id,
                            attempt_number=attempt,
                            status="running",
                        )
                    except Exception as ex:
                        self.logger.warning(f"[db_handler] set_context failed: {ex}")

                task.dag_id = dag_id
                task.execution_id = execution_id

                self.logger.info(
                    f"Executing task: {task_id} (attempt {attempt}/{max_retries + 1})"
                )

                executor_instance = self.executor_factory.get_executor(task.executor)
                task.execute(executor_instance)

                # --- SUCCESS ---
                task.status = TaskStatus.COMPLETED

                # ✅ SEMPRE aggiornare lo stato task (non dipende dagli attempt)
                with self.status_manager as sm:
                    sm.set_task_status(dag_id, task_id, "completed", execution_id)

                # 🔍 aggiornare l'attempt è best-effort
                if attempt_id:
                    try:
                        with self.status_manager as sm:
                            sm.complete_task_attempt(
                                attempt_id=attempt_id,
                                status="completed",
                            )
                    except Exception as ex:
                        self.logger.warning(
                            f"[task_attempts] complete_task_attempt failed for {task_id}: {ex}"
                        )

                if progress_tracker:
                    progress_tracker.increment_completed()
                if status_callback:
                    status_callback()

                return

            except BaseException as e:
                if isinstance(e, KeyboardInterrupt):
                    raise

                self.logger.error(f"Task {task_id} failed on attempt {attempt}: {e}")

                task.status = TaskStatus.FAILED

                # ✅ SEMPRE aggiornare lo stato task
                with self.status_manager as sm:
                    sm.set_task_status(dag_id, task_id, "failed", execution_id)

                # 🔍 attempt failed (best-effort)
                if attempt_id:
                    try:
                        with self.status_manager as sm:
                            sm.fail_task_attempt(attempt_id, error=str(e))
                    except Exception as ex:
                        self.logger.warning(
                            f"[task_attempts] fail_task_attempt failed for {task_id}: {ex}"
                        )

                if attempt <= max_retries:
                    # retry_count solo se davvero retryiamo
                    with self.status_manager as sm:
                        sm.increment_retry_count(dag_id, task_id, execution_id)

                    self.logger.info(
                        f"Retrying task {task_id} in {retry_delay} seconds "
                        f"(retry {attempt}/{max_retries})"
                    )

                    if retry_delay > 0:
                        time.sleep(retry_delay)

                    continue

                if status_callback:
                    status_callback()

                raise Exception(f"Task {task_id} failed: {e}") from e

            finally:
                # 🔒 force-close SOLO se l'attempt NON è stato chiuso esplicitamente
                if attempt_id and task.status == TaskStatus.RUNNING:
                    try:
                        with self.status_manager as sm:
                            sm.force_close_attempt_if_running(
                                attempt_id=attempt_id,
                                error="aborted",
                            )
                    except Exception as ex:
                        self.logger.warning(
                            f"[task_attempts] force_close_attempt_if_running failed for {task_id}: {ex}"
                        )

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
        sm.update_dag_execution_status(dag.dag_id, execution_id, "failed")

        # Mark pending tasks as skipped
        for task_id in pending_tasks:
            task = dag.tasks[task_id]
            task.status = TaskStatus.SKIPPED

            sm.set_task_status(
                dag.dag_id,
                task_id,
                "skipped",
                execution_id,
            )

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

    def _coerce_bool(self, v: Any) -> bool:
        """Coerce common YAML/JSON representations to bool safely."""
        if isinstance(v, bool):
            return v
        if v is None:
            return False
        if isinstance(v, (int, float)):
            return v != 0
        if isinstance(v, str):
            s = v.strip().lower()
            if s in ("true", "yes", "y", "1", "on"):
                return True
            if s in ("false", "no", "n", "0", "off", ""):
                return False
        return bool(v)

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


def evaluate_dependencies(
    task: BaseTask,
    upstream_statuses: Dict[str, str],
    sm,
    dag_id: str,
    execution_id: str,
) -> str:
    """
    Decide lo stato della task in base a:
    - dependency_policy (all/any/none)
    - stato delle dipendenze
    - condition dinamica basata sugli output delle task upstream

    Ritorna:
        "run"  -> task eseguibile
        "wait" -> task deve attendere
        "skip" -> task deve essere skippata
    """

    policy = getattr(task, "dependency_policy", "any")
    if hasattr(policy, "value"):
        policy = policy.value
    policy = str(policy).lower()
    dependencies = getattr(task, "dependencies", []) or []

    # NORMALIZZAZIONE CRITICA DELLA CONDITION
    raw_condition = getattr(task, "condition", None)
    condition = raw_condition if raw_condition not in ("", False) else None

    # ---------------------------------------------------------
    # 1) Nessuna dipendenza → valuta solo la condition
    # ---------------------------------------------------------
    if not dependencies:
        if condition is None:
            return "run"

        try:
            cond_value = eval(
                condition,
                {},
                {
                    "output_of": lambda tid: sm.get_task_output(
                        dag_id, tid, execution_id
                    )
                },
            )
            return "run" if cond_value else "skip"
        except Exception:
            # condition non valutabile → NON bloccare la DAG
            return "run"

    # ---------------------------------------------------------
    # 2) Gestione dipendenze (regole UNIVERSALI)
    # ---------------------------------------------------------
    statuses = [upstream_statuses.get(dep) for dep in dependencies]

    # Stato non ancora noto
    if any(s is None for s in statuses):
        return "wait"

    # Dipendenze non ancora terminali
    if any(s in ("pending", "running") for s in statuses):
        return "wait"

    # ---- policy ALL ----
    if policy == "all":
        if not all(s == "completed" for s in statuses):
            return "skip"

    # ---- policy ANY ----
    elif policy == "any":
        if not any(s == "completed" for s in statuses):
            return "skip"

    # ---- policy NONE ----
    elif policy == "none":
        pass  # tutte terminali → ok

    # ---------------------------------------------------------
    # 3) Gestione condition (POST-dipendenze)
    # ---------------------------------------------------------
    if condition is not None:

        def output_of(tid):
            try:
                return sm.get_task_output(dag_id, tid, execution_id)
            except Exception:
                return None

        try:
            cond_value = eval(condition, {}, {"output_of": output_of})
            if not cond_value:
                return "skip"
        except Exception:
            # condition non valutabile:
            # - policy none → NON blocca
            # - altre policy → skip
            return "run" if policy == "none" else "skip"

    # ---------------------------------------------------------
    # 4) Tutto OK → RUN
    # ---------------------------------------------------------
    return "run"
