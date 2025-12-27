import hashlib
import json
import random
import re
import string
import threading
import uuid
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, Union

import yaml

"""Internal status manager used by the server.

This module provides a simple SQLite-backed StatusManager used to persist
information about DAGs, executions, tasks and logs. The manager is used by
the server to record runtime status and expose queries for monitoring and
control operations.
"""

import json
import uuid

from sqlalchemy import create_engine, func
from sqlalchemy.orm import sessionmaker

from .models import (
    Base,
    DagORM,
    ExecutionORM,
    LogORM,
    TaskAttemptORM,
    TaskDependencyORM,
    TaskORM,
)

# Generatore di nomi in stile Docker - liste compatte di aggettivi e sostantivi
DOCKER_ADJECTIVES = """
amazing awesome blissful bold brave charming clever cool dazzling determined eager
ecstatic elegant epic exciting fantastic friendly gallant gentle gracious happy
hardcore inspiring jolly keen kind laughing loving lucid magical modest naughty
nervous nice objective optimistic peaceful pedantic pensive practical quirky
relaxed romantic serene sharp stoic sweet tender thirsty trusting unruffled
upbeat vibrant vigilant wonderful xenial youthful zealous zen
""".split()

DOCKER_NOUNS = """
albattani allen almeida antonelli archimedes ardinghelli aryabhata austin babbage
banach banzai bardeen bartik bassi beaver bell benz bhabha bhaskara black
blackburn blackwell bohr booth borg bose bouman boyd brahmagupta brattain brown
buck burnell cannon carson cartwright cerf chandrasekhar chaplygin chatelet
chatterjee chebyshev cohen chaum clarke colden cori cray curran curie darwin
davinci dewdney dhawan diffie dijkstra dirac driscoll dubinsky easley edison
einstein elbakyan elgamal elion ellis engelbart euclid euler faraday feistel
fermat fermi feynman franklin gagarin galileo galois ganguly gates gauss germain
goldberg goldstine goldwasser golick goodall gould greider grothendieck haibt
hamilton haslett hawking heisenberg hermann herschel hertz heyrovsky hodgkin
hofstadter hoover hopper hugle hypatia ishizaka jackson jang jemison jennings
jepsen johnson joliot jones kalam kapitsa kare keldysh keller kepler khorana
kilby kirch knuth kowalevski lalande lamarr lamport leakey leavitt lederberg
lehmann lewin lichterman liskov lovelace lumiere mahavira margulis matsumoto
maxwell mayer mccarthy mcclintock mclaren mclean mcnulty mendel mendeleev menshov
merkle mestorf mirzakhani moore morse murdoch moser napier nash neumann newton
nightingale nobel noether northcutt noyce panini pare pascal pasteur payne
perlman pike poincare poitras proskuriakova ptolemy raman ramanujan ride montalcini
ritchie robinson roentgen rosalind rubin saha sammet sanderson shannon shaw
shirley shockley shtern sinoussi snyder solomon spence stallman stonebraker
sutherland swanson swartz swirles taussig tereshkova tesla tharp thompson
torvalds tu turing varahamihira vaughan visvesvaraya volhard wescoff wilbur
wiles williams williamson wilson wing wozniak wright wu yalow yonath zhukovsky
""".split()


class StatusManager:
    """Manage persistent status for DAGs, executions, tasks and logs.

    The class provides methods to create and update execution records,
    set and query task statuses, record logs, and perform housekeeping.
    A single global instance is stored on the class and can be retrieved
    with `get_instance()` once initialized.
    """

    # 🆕 Class variable to hold the current singleton instance
    _instance = None

    # ----------------------------------------------------------------------

    # Construction and initialization

    def __init__(self, db_path: str):
        self.db_path = db_path
        self.engine = self._get_engine()
        self.Session = self._get_session_factory()
        self._initialize_tables_once()

        # 🆕 Store the global singleton instance
        StatusManager._instance = self

    # --------------------------- Critical notes ---------------------------
    #
    # - Sessions: this class uses SQLAlchemy `Session` and `Session.begin()`
    #   context managers to ensure transactional boundaries. Callers that
    #   need long-running or multi-step transactions should obtain their
    #   own session via `StatusManager.Session()`.
    #
    # - Concurrency: methods record `thread_id` and `pid` for executions
    #   and logs, but there is no explicit cross-process locking. If you
    #   run multiple processes against the same SQLite file consider using
    #   a server-backed DB (Postgres) to avoid SQLite locking/contention.
    #
    # - Task fallback: `get_task_status` intentionally falls back to the
    #   non-execution-scoped task record (execution_id=None) when a
    #   per-execution record is missing. This supports resume flows where
    #   tasks may have been persisted before the execution record existed.
    #
    # - Bulk updates: methods such as `mark_incomplete_tasks_as_failed`
    #   use `synchronize_session=False` for performance; this avoids
    #   keeping SQLAlchemy session state in sync and is acceptable here
    #   because sessions are short-lived and immediately committed.
    #
    # - ID generation: `generate_unique_dag_id` retries a fixed number of
    #   times then appends a random suffix. It is designed for convenience
    #   and not for cryptographic uniqueness.
    #
    # ---------------------------------------------------------------------

    # ----------------------------------------------------------------------
    # Private method: _get_engine
    def _get_engine(self):
        return create_engine(f"sqlite:///{self.db_path}")

    # ----------------------------------------------------------------------
    # Private method: _get_session_factory
    def _get_session_factory(self):
        return sessionmaker(bind=self.engine)

    # ----------------------------------------------------------------------
    # Private method: _initialize_tables_once
    def _initialize_tables_once(self):
        with self.engine.connect() as connection:
            Base.metadata.create_all(self.engine)

    # ----------------------------------------------------------------------
    # Helper: normalize fail_fast
    def _normalize_fail_fast(self, value: Any) -> str:
        if isinstance(value, str):
            return "ON" if value.strip().upper() == "ON" else "OFF"
        return "ON" if bool(value) else "OFF"

    def _generate_run_name(self) -> str:
        return f"{random.choice(DOCKER_ADJECTIVES)}_{random.choice(DOCKER_NOUNS)}"

    # ----------------------------------------------------------------------
    # Metodi del context manager

    # ----------------------------------------------------------------------
    # Metodo: __enter__
    def __enter__(self):
        """Enter a context and create a new DB session.

        Returns the manager instance with an open `session` attribute.
        """
        self.session = self.Session()
        return self

    # ----------------------------------------------------------------------
    # Metodo: __exit__
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Close the previously opened session when exiting the context."""
        if self.session:
            self.session.close()

    # ----------------------------------------------------------------------
    # Gestione stato task
    # ----------------------------------------------------------------------

    def set_task_status(
        self,
        dag_id: str,
        task_id: str,
        status: str,
        execution_id: str,
        is_final: Optional[bool] = None,
    ):
        """
        Update status of an existing task identified by (execution_id, task_id).
        DOES NOT create tasks anymore (tasks must be initialized explicitly).
        """
        with self.Session.begin() as session:
            task = (
                session.query(TaskORM)
                .filter_by(
                    dag_id=dag_id,
                    task_id=task_id,
                    execution_id=execution_id,
                )
                .first()
            )

            if not task:
                # Stato incoerente: il task doveva esistere
                return

            task.status = status
            task.thread_id = str(threading.current_thread().ident)

            if is_final is not None:
                task.is_final = 1 if is_final else 0

            if status == "running" and task.started_at is None:
                task.started_at = datetime.now()

            if status in ("completed", "failed", "skipped"):
                task.completed_at = datetime.now()

    # ----------------------------------------------------------------------

    def set_task_output(
        self,
        dag_id: str,
        task_id: str,
        execution_id: str,
        output: Any,
    ) -> None:
        if isinstance(output, (dict, list)):
            encoded = json.dumps(output)
            output_type = "json"
        else:
            encoded = str(output)
            output_type = "text"

        with self.Session.begin() as session:
            task = (
                session.query(TaskORM)
                .filter_by(
                    dag_id=dag_id,
                    task_id=task_id,
                    execution_id=execution_id,
                )
                .first()
            )

            if not task:
                return

            task.output = encoded
            task.output_type = output_type

    # ----------------------------------------------------------------------

    def get_task_status(
        self, dag_id: str, task_id: str, execution_id: str
    ) -> Optional[str]:
        with self.Session() as session:
            task = (
                session.query(TaskORM)
                .filter_by(
                    dag_id=dag_id,
                    task_id=task_id,
                    execution_id=execution_id,
                )
                .first()
            )
            return task.status if task else None

    # ----------------------------------------------------------------------
    # Metodo: get_task_output
    def get_task_output(self, dag_id: str, task_id: str, execution_id: str) -> Any:
        """Return the decoded output of a task, or None if not found."""
        with self.Session() as session:
            task = (
                session.query(TaskORM)
                .filter_by(dag_id=dag_id, task_id=task_id, execution_id=execution_id)
                .first()
            )

            if not task or task.output is None:
                return None

            try:
                return json.loads(task.output)
            except json.JSONDecodeError:
                return task.output  # fallback: raw string

    # ----------------------------------------------------------------------
    # Method: get_dag_status
    def get_dag_status(self, dag_id: str, execution_id: str = None) -> Dict[str, str]:
        """Return a mapping `{task_id: status}` for all tasks of a DAG.

        If `execution_id` is provided the query is scoped to that
        execution; otherwise all tasks for the DAG are returned.
        """
        with self.Session() as session:
            tasks = (
                session.query(TaskORM)
                .filter_by(dag_id=dag_id, execution_id=execution_id)
                .all()
            )
            return {task.task_id: task.status for task in tasks}

    # ----------------------------------------------------------------------
    # Method: reset_dag_status
    # Delete all task records (and executions when execution_id not provided).
    def reset_dag_status(self, dag_id: str, execution_id: str = None):
        """Reset stored status for a DAG.

        If `execution_id` is provided only tasks for that execution are
        removed. Otherwise all tasks and execution records for the DAG
        are deleted.
        """
        with self.Session.begin() as session:
            if execution_id:
                session.query(TaskORM).filter_by(
                    dag_id=dag_id, execution_id=execution_id
                ).delete()
            else:
                session.query(TaskORM).filter_by(dag_id=dag_id).delete()
                session.query(ExecutionORM).filter_by(dag_id=dag_id).delete()

    # ----------------------------------------------------------------------
    # Deliver status information about DAG
    # ----------------------------------------------------------------------

    def create_dag_execution(
        self,
        dag_id: str,
        execution_id: str,
        dag_filepath: Optional[str] = None,
        fail_fast: Any = False,
        run_name: Optional[str] = None,
        trigger_type: Optional[str] = None,
        triggered_by: Optional[str] = None,
    ) -> str:
        with self.Session.begin() as session:
            execution = session.query(ExecutionORM).filter_by(id=execution_id).first()

            if execution:
                return execution_id  # 🔒 già esistente → no-op

            execution = ExecutionORM(
                id=execution_id,
                dag_id=dag_id,
                run_name=run_name or self._generate_run_name(),
                status="queued",
                fail_fast=self._normalize_fail_fast(fail_fast),
                trigger_type=trigger_type,
                triggered_by=triggered_by,
                thread_id=str(threading.current_thread().ident),
                pid=threading.current_thread().ident,
            )
            session.add(execution)

            dag = session.query(DagORM).filter_by(id=dag_id).first()
            if not dag:
                raise RuntimeError(
                    f"DAG '{dag_id}' does not exist. "
                    "DAG must be created before creating executions."
                )

            return execution_id

    # ----------------------------------------------------------------------

    def create_dag_execution_with_status(
        self,
        dag_id: str,
        execution_id: str,
        status: str,
        fail_fast: Any = False,
        run_name: Optional[str] = None,
        trigger_type: Optional[str] = None,
        triggered_by: Optional[str] = None,
    ) -> str:
        with self.Session.begin() as session:
            execution = session.query(ExecutionORM).filter_by(id=execution_id).first()

            if execution:
                # 🔄 aggiorniamo solo lo status
                execution.status = status
                if status in ("queued", "running") and execution.started_at is None:
                    execution.started_at = datetime.now()
                    execution.thread_id = str(threading.current_thread().ident)
                return execution_id

            execution = ExecutionORM(
                id=execution_id,
                dag_id=dag_id,
                status=status,
                run_name=run_name or self._generate_run_name(),
                fail_fast=self._normalize_fail_fast(fail_fast),
                trigger_type=trigger_type,
                triggered_by=triggered_by,
                pid=threading.current_thread().ident,
            )

            if status in ("queued", "running"):
                execution.started_at = datetime.now()
                execution.thread_id = str(threading.current_thread().ident)

            session.add(execution)
            return execution_id

    # ----------------------------------------------------------------------

    def initialize_tasks_for_execution(
        self,
        dag_id: str,
        execution_id: str,
        tasks_or_task_ids: Union[Dict[str, Any], List[str]],
    ):
        """
        Insert TaskORM rows for a new execution.
        Tasks are created ONCE here, with UUID PK.
        """

        with self.Session.begin() as session:

            if hasattr(tasks_or_task_ids, "items"):
                iterable = tasks_or_task_ids.items()
            else:
                iterable = ((task_id, None) for task_id in tasks_or_task_ids)

            for i, (task_id, task_obj) in enumerate(iterable):
                max_retries = 0
                if task_obj is not None:
                    max_retries = int(getattr(task_obj, "retries", 0) or 0)

                task = TaskORM(
                    id=uuid.uuid4().hex,
                    task_id=task_id,
                    dag_id=dag_id,
                    execution_id=execution_id,
                    status="pending",
                    insertion_order=i,
                    is_final=bool(getattr(task_obj, "is_final", False)),
                    retry_count=0,
                    max_retries=max_retries,
                )
                session.add(task)

    # ----------------------------------------------------------------------

    def update_dag_execution_status(self, dag_id: str, execution_id: str, status: str):
        """Update the status of an existing execution record.

        If transitioning to `running` and `started_at` is missing, the
        method sets `started_at`, `thread_id` and `pid`. When a terminal
        status is set (`completed`, `failed`, `cancelled`) the
        `completed_at` timestamp is recorded.
        """
        with self.Session.begin() as session:
            execution = (
                session.query(ExecutionORM)
                .filter(ExecutionORM.id == execution_id)
                .first()
            )
            if execution:
                # If the DAG transitions to "running" and `started_at` is not set yet, set it now.
                if status == "running" and execution.started_at is None:
                    execution.started_at = datetime.now()
                    execution.thread_id = str(threading.current_thread().ident)
                    execution.pid = threading.current_thread().ident

                execution.status = status

                if status in ["completed", "failed"]:
                    execution.completed_at = datetime.now()

    # ----------------------------------------------------------------------

    def mark_incomplete_tasks_as_failed(self, dag_id: str, execution_id: str):
        """Mark all running/pending tasks for an execution as `failed`.

        Used during teardown or when an execution is aborted to ensure
        no tasks are left in non-terminal states. For performance the
        update uses `synchronize_session=False`.
        """
        with self.Session.begin() as session:
            session.query(TaskORM).filter(
                TaskORM.dag_id == dag_id,
                TaskORM.execution_id == execution_id,
                TaskORM.status.in_(["running", "pending"]),
            ).update(
                {TaskORM.status: "failed", TaskORM.completed_at: datetime.now()},
                synchronize_session=False,
            )

    # ----------------------------------------------------------------------

    def get_running_dags(self) -> List[Dict[str, Any]]:
        """Return a list of currently running executions with metadata.

        Each item contains `dag_id`, `execution_id`, `started_at`,
        `thread_id` and `pid`.
        """
        with self.Session() as session:
            executions = session.query(ExecutionORM).filter_by(status="running").all()
            return [
                {
                    "dag_id": e.dag_id,
                    "execution_id": e.id,
                    "started_at": e.started_at.isoformat() if e.started_at else None,
                    "thread_id": e.thread_id,
                    "pid": e.pid,
                }
                for e in executions
            ]

    # ----------------------------------------------------------------------

    def get_dags_by_status(self, status: str) -> List[Dict[str, Any]]:
        """Return executions filtered by `status` with relevant metadata."""
        with self.Session() as session:
            executions = session.query(ExecutionORM).filter_by(status=status).all()
            return [
                {
                    "dag_id": e.dag_id,
                    "execution_id": e.id,
                    "started_at": e.started_at.isoformat() if e.started_at else None,
                    "completed_at": (
                        e.completed_at.isoformat() if e.completed_at else None
                    ),
                    "thread_id": e.thread_id,
                    "pid": e.pid,
                }
                for e in executions
            ]

    # ----------------------------------------------------------------------

    def get_all_dags(self) -> List[Dict[str, Any]]:
        """Return a list of all known DAGs and their latest execution.

        Each entry includes the DAG id and a summary of the most recent
        execution (if any).
        """
        with self.Session() as session:
            dags = session.query(DagORM).all()
            result = []
            for dag in dags:
                latest_execution = (
                    session.query(ExecutionORM)
                    .filter_by(dag_id=dag.id)
                    .order_by(ExecutionORM.started_at.desc())
                    .first()
                )
                result.append(
                    {
                        "dag_id": dag.id,
                        "execution_id": (
                            latest_execution.id if latest_execution else None
                        ),
                        "status": (
                            latest_execution.status if latest_execution else "queued"
                        ),
                        "started_at": (
                            latest_execution.started_at.isoformat()
                            if latest_execution and latest_execution.started_at
                            else None
                        ),
                        "completed_at": (
                            latest_execution.completed_at.isoformat()
                            if latest_execution and latest_execution.completed_at
                            else None
                        ),
                        "thread_id": (
                            latest_execution.thread_id if latest_execution else None
                        ),
                        "pid": latest_execution.pid if latest_execution else None,
                    }
                )
            return result

    # ----------------------------------------------------------------------

    def get_dag_summary(self) -> Dict[str, Any]:
        """Return aggregated counts for executions and DAGs.

        The result contains `total_executions`, `unique_dags` and a
        mapping `status_counts` showing how many executions are in each
        status.
        """
        with self.Session() as session:
            status_counts = (
                session.query(ExecutionORM.status, func.count(ExecutionORM.status))
                .group_by(ExecutionORM.status)
                .all()
            )
            total_count = session.query(ExecutionORM).count()
            unique_dags = session.query(DagORM).count()
            return {
                "total_executions": total_count,
                "unique_dags": unique_dags,
                "status_counts": dict(status_counts),
            }

    # ----------------------------------------------------------------------
    # Method: get_dag_history
    def get_dag_history(self, dag_id: str) -> List[Dict[str, Any]]:
        """Return the execution history for a DAG ordered by start time.

        Each entry contains `execution_id`, `status`, timestamps and
        thread/pid information.
        """
        with self.Session() as session:
            executions = (
                session.query(ExecutionORM)
                .filter_by(dag_id=dag_id)
                .order_by(ExecutionORM.started_at.desc())
                .all()
            )
            return [
                {
                    "execution_id": e.id,
                    "status": e.status,
                    "started_at": e.started_at.isoformat() if e.started_at else None,
                    "completed_at": (
                        e.completed_at.isoformat() if e.completed_at else None
                    ),
                    "thread_id": e.thread_id,
                    "pid": e.pid,
                }
                for e in executions
            ]

    # ----------------------------------------------------------------------
    # Method: cleanup_old_executions
    def cleanup_old_executions(self, days_to_keep: int = 30):
        """Delete executions older than `days_to_keep` days and return count.

        Use with care: this permanently removes execution and related task
        records.
        """
        with self.Session.begin() as session:
            cutoff_date = datetime.now() - timedelta(days=days_to_keep)
            executions_to_delete = (
                session.query(ExecutionORM)
                .filter(ExecutionORM.started_at < cutoff_date)
                .all()
            )
            for execution in executions_to_delete:
                session.delete(execution)
            return len(executions_to_delete)

    # ----------------------------------------------------------------------
    # Method: cancel_dag_execution
    def cancel_dag_execution(self, dag_id: str, execution_id: str = None) -> bool:
        """Cancel a running execution and mark its tasks as `cancelled`.

        If `execution_id` is None the first running execution for the DAG
        is targeted.
        """
        with self.Session.begin() as session:
            query = session.query(ExecutionORM).filter_by(
                dag_id=dag_id, status="running"
            )
            if execution_id:
                query = query.filter_by(id=execution_id)
            execution = query.first()
            if execution:
                # Mark the execution as failed (schema does not include "cancelled")
                execution.status = "failed"
                execution.completed_at = datetime.now()

                # Mark all incomplete tasks (running, pending) as skipped
                session.query(TaskORM).filter(
                    TaskORM.dag_id == dag_id,
                    TaskORM.execution_id == execution.id,
                    TaskORM.status.in_(["running", "pending"]),
                ).update(
                    {TaskORM.status: "skipped", TaskORM.completed_at: datetime.now()},
                    synchronize_session=False,
                )

                return True
            return False

    # ----------------------------------------------------------------------
    # Method: get_dag_execution_details
    def get_dag_execution_details(
        self, dag_id: str, execution_id: str = None
    ) -> Dict[str, Any]:
        """Return detailed information for a specific execution.

        If `execution_id` is omitted the latest execution is returned. The
        result includes the list of tasks with their timestamps and
        statuses.
        """
        with self.Session() as session:
            query = session.query(ExecutionORM).filter_by(dag_id=dag_id)
            if execution_id:
                query = query.filter_by(id=execution_id)
            else:
                query = query.order_by(ExecutionORM.started_at.desc())
            execution = query.first()

            if not execution:
                return {}

            tasks = (
                session.query(TaskORM)
                .filter_by(dag_id=dag_id, execution_id=execution.id)
                .order_by(TaskORM.insertion_order)
                .all()
            )
            return {
                "execution_id": execution.id,
                "status": execution.status,
                "started_at": (
                    execution.started_at.isoformat() if execution.started_at else None
                ),
                "completed_at": (
                    execution.completed_at.isoformat()
                    if execution.completed_at
                    else None
                ),
                "thread_id": execution.thread_id,
                "pid": execution.pid,
                "tasks": [
                    {
                        "task_id": task.task_id,
                        "status": task.status,
                        "started_at": (
                            task.started_at.isoformat() if task.started_at else None
                        ),
                        "completed_at": (
                            task.completed_at.isoformat() if task.completed_at else None
                        ),
                        "thread_id": task.thread_id,
                    }
                    for task in tasks
                ],
            }

    # ----------------------------------------------------------------------

    def log_message(
        self,
        dag_id: str,
        execution_run_name: str,
        task_id: Optional[str],
        attempt_number: Optional[int],
        status: Optional[str],
        level: str,
        message: str,
        timestamp: Optional[datetime] = None,
    ):
        with self.Session.begin() as session:
            log = LogORM(
                id=uuid.uuid4().hex,
                dag_id=dag_id,
                execution_id=execution_run_name,  # <-- qui dentro salviamo run_name
                task_id=task_id,  # <-- task_id string
                attempt_id=attempt_number,  # <-- attempt_number
                status=status,  # <-- status dell'attempt
                level=level,
                message=message,
                timestamp=timestamp or datetime.now(),
                thread_id=str(threading.current_thread().ident),
                pid=threading.current_thread().ident,
            )
            session.add(log)

    # ----------------------------------------------------------------------

    def add_log(
        self,
        dag_id: str,
        message: str,
        level: str = "INFO",
        timestamp: Optional[datetime] = None,
        # --- NEW preferred ---
        execution_run_name: Optional[str] = None,
        attempt_number: Optional[int] = None,
        status: Optional[str] = None,
        task_id: Optional[str] = None,
        # --- LEGACY (compat) ---
        execution_id: Optional[str] = None,
    ):
        """
        Backward-compatible log helper.

        Preferred: pass execution_run_name.
        Legacy: pass execution_id and we resolve run_name from DB.
        """

        # Resolve run_name if caller passed execution_id (legacy)
        if not execution_run_name and execution_id:
            resolved = self.get_execution_run_name(execution_id)
            # fallback ultra-safe: if not found, keep execution_id so logs are not lost
            execution_run_name = resolved or execution_id

        # Ensure execution_id is available (reverse resolve)
        if execution_id is None and execution_run_name:
            execution_id = self.get_execution_id_from_run_name(execution_run_name)

        # If still missing, make it explicit but don't crash
        execution_run_name = execution_run_name or "unknown_run"

        # AUTO-FILL attempt_number e status da task_attempts

        if task_id and execution_id and (attempt_number is None or status is None):
            session = self.Session()

            attempt = (
                session.query(TaskAttemptORM)
                .filter(
                    TaskAttemptORM.execution_id == execution_id,
                    TaskAttemptORM.task_pk == task_id,
                )
                .order_by(TaskAttemptORM.attempt_number.desc())
                .first()
            )

            if attempt:
                if attempt_number is None:
                    attempt_number = attempt.attempt_number

                if status is None:
                    status = attempt.status

            session.close()

        # AUTO-FILL attempt_number e status da task_attempts
        # (serve risolvere task_pk dalla tabella tasks)
        if task_id and execution_id and (attempt_number is None or status is None):
            with self.Session.begin() as session:
                # 1) trova la riga task (serve per ricavare task_pk = tasks.id)
                task_row = (
                    session.query(TaskORM)
                    .filter_by(
                        dag_id=dag_id,
                        task_id=task_id,  # <-- stringa ("start", "final", ...)
                        execution_id=execution_id,  # <-- id vero di executions.id
                    )
                    .first()
                )

                if task_row:
                    # 2) trova l'ultimo attempt per quella task_pk
                    attempt = (
                        session.query(TaskAttemptORM)
                        .filter(
                            TaskAttemptORM.execution_id == execution_id,
                            TaskAttemptORM.task_pk
                            == task_row.id,  # <-- QUESTA è la chiave giusta
                        )
                        .order_by(TaskAttemptORM.attempt_number.desc())
                        .first()
                    )

                    if attempt:
                        if attempt_number is None:
                            attempt_number = attempt.attempt_number
                        if status is None:
                            status = attempt.status

        self.log_message(
            dag_id=dag_id,
            execution_run_name=execution_run_name,
            task_id=task_id,
            attempt_number=attempt_number,
            status=status,
            level=level,
            message=message,
            timestamp=timestamp,
        )

    # ----------------------------------------------------------------------

    def get_execution_logs(
        self, dag_id: str, execution_id: str = None, limit: int = 100
    ) -> List[Dict[str, Any]]:
        with self.Session() as session:
            query = session.query(LogORM).filter_by(dag_id=dag_id)
            if execution_id:
                query = query.filter_by(execution_id=execution_id)

            logs = query.order_by(LogORM.timestamp.desc()).limit(limit).all()

            result = []
            for log in logs:
                task_id = None
                if log.task_pk:
                    task = session.get(TaskORM, log.task_pk)
                    task_id = task.task_id if task else None

                result.append(
                    {
                        "task_id": task_id,
                        "level": log.level,
                        "message": log.message,
                        "timestamp": log.timestamp.isoformat(),
                        "thread_id": log.thread_id,
                    }
                )
            return result

    # ----------------------------------------------------------------------
    # Method: validate_dag_id
    def validate_dag_id(self, dag_id: str) -> bool:
        """Return True if `dag_id` is a non-empty alphanumeric identifier.

        Allowed characters are letters, digits, underscores and hyphens.
        """
        if not dag_id:
            return False
        return bool(re.match(r"^[a-zA-Z0-9_-]+$", dag_id))

    # ----------------------------------------------------------------------
    # Method: check_dag_id_uniqueness
    def check_dag_id_uniqueness(self, dag_id: str) -> bool:
        """Return True if no DagORM with `dag_id` exists in the DB."""
        with self.Session() as session:
            return session.query(DagORM).filter_by(id=dag_id).first() is None

    # ----------------------------------------------------------------------
    # Method: generate_unique_dag_id
    def generate_unique_dag_id(self) -> str:
        """Generate a human-friendly, likely-unique dag id.

        It tries `max_attempts` times to pick a combination that does not
        collide with existing DAG ids. If all attempts fail it appends a
        short random suffix.
        """
        max_attempts = 100
        for _ in range(max_attempts):
            dag_id = f"{random.choice(DOCKER_ADJECTIVES)}_{random.choice(DOCKER_NOUNS)}"
            if self.check_dag_id_uniqueness(dag_id):
                return dag_id
        base_name = f"{random.choice(DOCKER_ADJECTIVES)}_{random.choice(DOCKER_NOUNS)}"
        suffix = "".join(random.choices(string.ascii_lowercase + string.digits, k=6))
        return f"{base_name}_{suffix}"

    # ----------------------------------------------------------------------

    def get_latest_execution(self, dag_id: str) -> Optional[Dict[str, Any]]:
        """Return a summary dict for the latest execution of `dag_id`.

        Returns `None` if no execution exists.
        """
        with self.Session() as session:
            execution = (
                session.query(ExecutionORM)
                .filter_by(dag_id=dag_id)
                .order_by(ExecutionORM.started_at.desc())
                .first()
            )
            if execution:
                return {
                    "execution_id": execution.id,
                    "status": execution.status,
                    "started_at": (
                        execution.started_at.isoformat()
                        if execution.started_at
                        else None
                    ),
                    "completed_at": (
                        execution.completed_at.isoformat()
                        if execution.completed_at
                        else None
                    ),
                    "thread_id": execution.thread_id,
                    "pid": execution.pid,
                }
            return None

    # ----------------------------------------------------------------------

    def get_execution_run_name(self, execution_id: str) -> Optional[str]:
        """Return run_name for a given execution id."""
        if not execution_id:
            return None
        with self.Session() as session:
            e = session.query(ExecutionORM).filter_by(id=execution_id).first()
            return e.run_name if e else None

    # ----------------------------------------------------------------------

    def save_dag_definition(self, dag, dag_filepath: Optional[str] = None):
        """
        Persist DAG metadata.
        - created_at: set once, on first sight
        - updated_at: set ONLY if definition_hash changes
        """

        # 1️⃣ Calcolo hash stabile del file YAML
        definition_hash = None
        if dag_filepath:
            with open(dag_filepath, "r") as f:
                raw = f.read()
            definition_hash = hashlib.sha256(raw.encode()).hexdigest()

        now = datetime.now()

        with self.Session.begin() as session:
            dag_orm = session.query(DagORM).filter_by(id=dag.dag_id).first()

            # --------------------------------------------------
            # Caso: DAG mai vista prima
            # --------------------------------------------------
            if not dag_orm:
                dag_orm = DagORM(
                    id=dag.dag_id,
                    definition_hash=definition_hash or "",
                    dag_filepath=dag_filepath,
                    created_at=now,
                    updated_at=now,
                    is_active=1,
                )
                session.add(dag_orm)
                return

            # --------------------------------------------------
            # Caso: DAG già esistente
            # --------------------------------------------------

            else:
                changed = False

                if definition_hash and definition_hash != dag_orm.definition_hash:
                    dag_orm.definition_hash = definition_hash
                    changed = True

                if dag_filepath and dag_filepath != dag_orm.dag_filepath:
                    dag_orm.dag_filepath = dag_filepath

                if changed:
                    dag_orm.updated_at = now

            # --------------------------------------------------
            # SEMPRE ESEGUITO
            # --------------------------------------------------

            self.save_task_dependencies_from_dag(dag)

    # ----------------------------------------------------------------------

    def get_dag_definition(self, dag_id: str) -> Optional[Dict]:
        with self.Session() as session:
            dag = session.query(DagORM).filter_by(id=dag_id).first()
            if not dag or not dag.dag_filepath:
                return None

            try:
                with open(dag.dag_filepath, "r") as f:
                    return yaml.safe_load(f)
            except Exception:
                return None

    def get_dag_filepath(self, dag_id: str) -> Optional[str]:
        """Return the stored DAG YAML path for a DAG id."""
        with self.Session() as session:
            dag = session.query(DagORM).filter_by(id=dag_id).first()
            return dag.dag_filepath if dag else None

    # ----------------------------------------------------------------------
    # Method: delete_dag
    def delete_dag(self, dag_id: str) -> int:
        """Delete the DAG record (and by cascade its executions/tasks if set).

        Returns 1 if a record was deleted, 0 otherwise.
        """
        with self.Session.begin() as session:
            dag = session.query(DagORM).filter_by(id=dag_id).first()
            if dag:
                session.delete(dag)
                return 1
            return 0

    # ----------------------------------------------------------------------
    # Method: get_instance (class method)
    @classmethod
    def get_instance(cls):
        """Return the previously initialized global StatusManager.

        Raises a RuntimeError if the manager has not been initialized.
        """
        if cls._instance is None:
            raise RuntimeError("StatusManager has not been initialized yet.")
        return cls._instance

    def has_any_failed_task(self, dag_id: str, execution_id: str) -> bool:
        tasks = self.get_tasks_for_execution(dag_id, execution_id)
        return any(t["status"] == "failed" for t in tasks)

    def update_execution_status_from_final_task(
        self,
        dag_id: str,
        execution_id: str,
    ) -> Optional[str]:

        # 1️⃣ risolvi final task + exit-task
        self.resolve_final_tasks(dag_id, execution_id)
        exit_task = self.find_exit_task(dag_id, execution_id)

        if not exit_task:
            return None

        # 2️⃣ mappa status task → status execution
        if exit_task.status == "completed":
            new_status = "completed"
        elif exit_task.status == "failed":
            new_status = "failed"
        elif exit_task.status == "skipped":
            # caso all-skipped
            new_status = "failed"
        else:
            # fallback ultra-difensivo (non dovrebbe mai succedere)
            new_status = "failed"

        # 3️⃣ persistenza
        with self.Session.begin() as session:
            execution = (
                session.query(ExecutionORM)
                .filter_by(dag_id=dag_id, id=execution_id)
                .first()
            )
            if not execution:
                return None

            failed_count = (
                session.query(TaskORM)
                .filter_by(dag_id=dag_id, execution_id=execution_id, status="failed")
                .count()
            )
            skipped_count = (
                session.query(TaskORM)
                .filter_by(dag_id=dag_id, execution_id=execution_id, status="skipped")
                .count()
            )

            execution.status = new_status
            execution.failed_tasks = failed_count
            execution.skipped_tasks = skipped_count
            execution.completed_at = datetime.now()
            return new_status

    def is_fail_fast_enabled(self, dag_id: str) -> bool:
        definition = self.get_dag_definition(dag_id)
        if not definition:
            return False
        dag_def = definition.get("dag", definition)
        return bool(dag_def.get("fail_fast", False))

    def resolve_final_tasks(self, dag_id: str, execution_id: str) -> None:
        """
        Determina e normalizza tasks.is_final secondo le regole definitive.
        """

        with self.Session.begin() as session:
            tasks = (
                session.query(TaskORM)
                .filter_by(dag_id=dag_id, execution_id=execution_id)
                .all()
            )

            if not tasks:
                return

            task_by_id = {t.task_id: t for t in tasks}

            dag_def = self.get_dag_definition(dag_id)
            dag_def = dag_def.get("dag", dag_def) if dag_def else {}
            dag_tasks = dag_def.get("tasks", {}) if dag_def else {}
            if isinstance(dag_tasks, list):
                dag_tasks = {
                    t.get("task_id"): t for t in dag_tasks if isinstance(t, dict)
                }

            # -------------------------------------------------
            # 1️⃣ Individua finali STRUTTURALI
            # -------------------------------------------------

            # a) finali esplicite nel DAG
            explicit_final_ids = {
                tid for tid, t in dag_tasks.items() if bool(t.get("is_final", False))
            }

            if explicit_final_ids:
                structural_final_ids = explicit_final_ids
            else:
                # b) sink nodes (non dipendenze di nessuno)
                all_deps = set()
                for t in dag_tasks.values():
                    for dep in t.get("dependencies", []) or []:
                        all_deps.add(dep)

                structural_final_ids = {
                    tid for tid in dag_tasks.keys() if tid not in all_deps
                }

            structural_finals = [
                task_by_id[tid] for tid in structural_final_ids if tid in task_by_id
            ]

            # -------------------------------------------------
            # Helper: risalita dipendenze
            # -------------------------------------------------
            def find_upstream_with_started_at(task, visited=None):
                if visited is None:
                    visited = set()
                if task.id in visited:
                    return None
                visited.add(task.id)

                deps = dag_tasks.get(task.task_id, {}).get("dependencies", [])

                candidates = []
                for dep_id in deps:
                    upstream = task_by_id.get(dep_id)
                    if not upstream:
                        continue
                    if upstream.started_at:
                        candidates.append(upstream)
                    else:
                        c = find_upstream_with_started_at(upstream, visited)
                        if c:
                            candidates.append(c)

                return (
                    max(candidates, key=lambda t: t.started_at) if candidates else None
                )

            # -------------------------------------------------
            # 2️⃣ Normalizzazione DB
            # -------------------------------------------------
            for t in tasks:
                t.is_final = False

            # -------------------------------------------------
            # 3️⃣ Validazione temporale
            # -------------------------------------------------
            final_candidates = []

            for task in structural_finals:
                if task.started_at:
                    final_candidates.append(task)
                else:
                    upstream = find_upstream_with_started_at(task)
                    if upstream:
                        final_candidates.append(upstream)

            # -------------------------------------------------
            # 4️⃣ Fallback estremo
            # -------------------------------------------------
            if not final_candidates:
                fallback = max(tasks, key=lambda t: t.insertion_order)
                fallback.is_final = True
                return

            # -------------------------------------------------
            # 5️⃣ Scrittura finale
            # -------------------------------------------------
            for t in final_candidates:
                t.is_final = True

    def find_exit_task(self, dag_id: str, execution_id: str) -> Optional[TaskORM]:
        """
        Ritorna la exit-task per l'execution.
        Assume che resolve_final_tasks() sia già stata chiamata.
        """

        with self.Session() as session:
            final_tasks = (
                session.query(TaskORM)
                .filter(
                    TaskORM.dag_id == dag_id,
                    TaskORM.execution_id == execution_id,
                    TaskORM.is_final.is_(True),
                )
                .all()
            )

            if not final_tasks:
                return None

            with_started = [t for t in final_tasks if t.started_at]
            if with_started:
                return max(with_started, key=lambda t: t.started_at)

            return max(final_tasks, key=lambda t: t.insertion_order)

    def update_execution_trigger_context(
        self,
        dag_id: str,
        execution_id: str,
        trigger_type: str,
        triggered_by: str,
    ) -> None:
        with self.Session.begin() as session:
            execution = (
                session.query(ExecutionORM)
                .filter_by(id=execution_id, dag_id=dag_id)
                .first()
            )
            if not execution:
                return

            execution.trigger_type = trigger_type
            execution.triggered_by = triggered_by

    def increment_retry_count(
        self,
        dag_id: str,
        task_id: str,
        execution_id: str,
    ) -> None:
        with self.Session.begin() as session:
            task = (
                session.query(TaskORM)
                .filter_by(
                    dag_id=dag_id,
                    task_id=task_id,
                    execution_id=execution_id,
                )
                .first()
            )
            if not task:
                return

            task.retry_count += 1

    def create_task_attempt(
        self,
        dag_id: str,
        task_id: str,
        execution_id: str,
        attempt_number: int,
    ) -> str:
        with self.Session.begin() as session:
            task = (
                session.query(TaskORM)
                .filter_by(
                    dag_id=dag_id,
                    task_id=task_id,
                    execution_id=execution_id,
                )
                .first()
            )

            if not task:
                return None

            attempt = TaskAttemptORM(
                id=uuid.uuid4().hex,
                task_pk=task.id,
                execution_id=execution_id,
                attempt_number=attempt_number,
                status="running",
                started_at=datetime.now(),
                pid=threading.current_thread().ident,
                thread_id=str(threading.current_thread().ident),
            )

            session.add(attempt)
            return attempt.id

    def complete_task_attempt(
        self, attempt_id: str, status: str, error: Optional[str] = None
    ):
        if not attempt_id:
            return

        # ci servono per il log finale, ma li prendiamo senza nested transaction
        dag_id = None
        execution_id = None
        task_id = None

        with self.Session.begin() as session:
            attempt = session.get(TaskAttemptORM, attempt_id)
            if not attempt:
                return

            # aggiorna attempt
            attempt.status = status
            attempt.completed_at = datetime.now()
            attempt.error = error

            # prepara info per log finale (senza scriverlo qui!)
            task = session.get(TaskORM, attempt.task_pk)
            if task:
                dag_id = task.dag_id
                execution_id = attempt.execution_id
                task_id = task.task_id

        # ✅ FUORI dalla transazione: niente lock annidati
        if dag_id and execution_id and task_id:
            level = "INFO" if status == "completed" else "ERROR"
            self.add_log(
                dag_id=dag_id,
                execution_id=execution_id,
                task_id=task_id,
                message=f"[Task] finished with status {status}",
                level=level,
            )

    def fail_task_attempt(self, attempt_id: str, error: str):
        self.complete_task_attempt(
            attempt_id=attempt_id,
            status="failed",
            error=error,
        )

    def force_close_attempt_if_running(self, attempt_id: str, error: str = "aborted"):
        if not attempt_id:
            return

        with self.Session.begin() as session:
            attempt = session.get(TaskAttemptORM, attempt_id)
            if not attempt:
                return

            # 🔒 NO-OP se già chiuso
            if attempt.completed_at is not None:
                return

            if attempt.status != "running":
                return

            # 🔥 chiusura forzata SOLO se ancora running
            attempt.status = "failed"
            attempt.completed_at = datetime.now()
            attempt.error = error

    def save_task_dependencies_from_dag(self, dag) -> None:
        """
        Persist structural task dependencies for a DAG.
        One row per task (node-centric DAG representation).
        """

        print(">>> save_task_dependencies_from_dag CALLED")
        print(">>> dag =", dag)
        print(">>> dag.tasks =", getattr(dag, "tasks", None))

        dag_id = dag.dag_id

        # -------------------------------------------------
        # 1️⃣ Costruzione mappe DAL DAG IN MEMORIA
        # -------------------------------------------------

        upstream_map: dict[str, list[str]] = {}
        downstream_map: dict[str, list[str]] = {}
        task_meta: dict[str, dict] = {}

        for task_id, task in dag.tasks.items():
            deps = list(getattr(task, "dependencies", []) or [])

            upstream_map[task_id] = deps
            downstream_map.setdefault(task_id, [])

            task_meta[task_id] = {
                "condition": getattr(task, "condition", None),
                "dependency_policy": getattr(task, "dependency_policy", "all"),
            }

            for upstream in deps:
                downstream_map.setdefault(upstream, []).append(task_id)

        # -------------------------------------------------
        # 2️⃣ Persistenza DB (idempotente)
        # -------------------------------------------------

        with self.Session.begin() as session:
            session.query(TaskDependencyORM).filter_by(dag_id=dag_id).delete()

            # Ordine canonico: tasks.insertion_order
            task_rows = (
                session.query(TaskORM.task_id)
                .filter(TaskORM.dag_id == dag_id)
                .order_by(TaskORM.insertion_order)
                .all()
            )

            all_task_ids = [row.task_id for row in task_rows]

            for task_id in all_task_ids:
                dep = TaskDependencyORM(
                    id=uuid.uuid4().hex,
                    dag_id=dag_id,
                    task_id=task_id,
                    upstream_task_ids=json.dumps(upstream_map.get(task_id, [])),
                    downstream_task_ids=json.dumps(downstream_map.get(task_id, [])),
                    condition=task_meta.get(task_id, {}).get("condition"),
                    dependency_policy=task_meta.get(task_id, {}).get(
                        "dependency_policy", "all"
                    ),
                )
                session.add(dep)
