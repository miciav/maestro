import json
import random
import re
import string
import threading
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, Union

"""Internal status manager used by the server.

This module provides a simple SQLite-backed StatusManager used to persist
information about DAGs, executions, tasks and logs. The manager is used by
the server to record runtime status and expose queries for monitoring and
control operations.
"""

from sqlalchemy import create_engine, func
from sqlalchemy.orm import sessionmaker

from .models import Base, DagORM, ExecutionORM, LogORM, TaskORM

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
    # Metodo: set_task_status
    def set_task_status(
        self,
        dag_id: str,
        task_id: str,
        status: str,
        execution_id: str = None,
        is_final: bool = False,
    ):
        """Create or update a task record and set its status.

        If the task does not exist it is created. When a task becomes
        `running` the `started_at` timestamp is set; when it reaches a
        terminal state (`completed`, `failed`, `cancelled`, `skipped`)
        the `completed_at` timestamp is set.
        """
        with self.Session.begin() as session:
            task = (
                session.query(TaskORM)
                .filter_by(dag_id=dag_id, id=task_id, execution_id=execution_id)
                .first()
            )

            if not task:
                task = TaskORM(
                    dag_id=dag_id,
                    id=task_id,
                    execution_id=execution_id,
                    is_final="True" if is_final else "False",
                )
                session.add(task)

            task.status = status
            task.is_final = "True" if is_final else "False"
            task.thread_id = str(threading.current_thread().ident)

            if status == "running":
                task.started_at = datetime.now()
            elif status in ["completed", "failed", "cancelled", "skipped"]:
                task.completed_at = datetime.now()

    # ----------------------------------------------------------------------
    # Metodo: set_task_output
    def set_task_output(
        self,
        dag_id: str,
        task_id: str,
        execution_id: str,
        output: Any,
    ) -> None:
        """
        Salva l'output di un task nella colonna `output` della tabella tasks.

        NOTA IMPORTANTE:
        - NON crea nuove righe nella tabella tasks.
        - Aggiorna SOLO il record esistente identificato da
          (dag_id, task_id, execution_id).
        """
        # serializza in JSON se è dict/list, altrimenti stringa semplice
        if isinstance(output, (dict, list)):
            encoded = json.dumps(output)
        else:
            encoded = str(output)

        with self.Session.begin() as session:
            task = (
                session.query(TaskORM)
                .filter_by(dag_id=dag_id, id=task_id, execution_id=execution_id)
                .first()
            )

            if not task:
                # Non creiamo una nuova riga qui: se non troviamo il task,
                # c'è qualcosa di incoerente a monte.
                # Volendo, potresti loggare un warning.
                return

            task.output = encoded

    # ----------------------------------------------------------------------
    # Metodo: get_task_status
    def get_task_status(
        self, dag_id: str, task_id: str, execution_id: str = None
    ) -> Optional[str]:
        """Return the status for a specific task.

        Looks up the task by `dag_id` and `task_id`. If an `execution_id`
        is provided the lookup is scoped to that execution; if the record
        is missing for the given execution the method falls back to the
        globally-scoped task (execution_id=None) to preserve resume flows.

        Returns the status string, or `None` if the task is not found.
        """
        with self.Session() as session:
            task = (
                session.query(TaskORM)
                .filter_by(dag_id=dag_id, id=task_id, execution_id=execution_id)
                .first()
            )

            if not task and execution_id is not None:
                # Ripiega sullo stato del task senza scoping per esecuzione
                # quando manca un record specifico per l'esecuzione. Questo
                # replica il comportamento delle implementazioni precedenti
                # basate su file e mantiene funzionanti i flussi di resume
                # quando i task sono stati persistiti prima della creazione
                # del record di esecuzione.
                task = (
                    session.query(TaskORM)
                    .filter_by(dag_id=dag_id, id=task_id, execution_id=None)
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
                .filter_by(dag_id=dag_id, id=task_id, execution_id=execution_id)
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
            return {task.id: task.status for task in tasks}

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
    # Method: create_dag_execution
    def create_dag_execution(
        self, dag_id: str, execution_id: str, dag_filepath: Optional[str] = None
    ) -> str:
        """Create a new Execution record for `dag_id` and mark it running.

        The method records start time, thread id and pid. If the DAG record
        does not exist it is created. Returns the `execution_id`.
        """
        with self.Session.begin() as session:
            execution = ExecutionORM(
                id=execution_id,
                dag_id=dag_id,
                status="running",
                started_at=datetime.now(),
                thread_id=str(threading.current_thread().ident),
                pid=threading.current_thread().ident,
            )
            session.add(execution)
            dag = session.query(DagORM).filter_by(id=dag_id).first()
            if not dag:
                dag = DagORM(id=dag_id, dag_filepath=dag_filepath)
                session.add(dag)
            elif dag_filepath:
                dag.dag_filepath = dag_filepath
            return execution_id

    # ----------------------------------------------------------------------
    # Method: create_dag_execution_with_status
    def create_dag_execution_with_status(
        self, dag_id: str, execution_id: str, status: str
    ) -> str:
        """Create a new Execution record with an explicit status.

        This variant allows creating executions in non-running states
        (for example `created` or `failed`). If `status` is not `created`
        the `started_at`/`thread_id` are set as well.
        """
        with self.Session.begin() as session:
            execution = ExecutionORM(
                id=execution_id,
                dag_id=dag_id,
                status=status,
                pid=threading.current_thread().ident,
            )
            if status != "created":
                execution.started_at = datetime.now()
                execution.thread_id = str(threading.current_thread().ident)
            session.add(execution)
            return execution_id

    # ----------------------------------------------------------------------
    # Method: initialize_tasks_for_execution

    def initialize_tasks_for_execution(
        self,
        dag_id: str,
        execution_id: str,
        tasks_or_task_ids: Union[Dict[str, Any], List[str]],
    ):
        """Insert initial TaskORM rows for a new execution, including is_final when available.

        Backward compatible:
        - if tasks_or_task_ids is a dict: expects {task_id: task_obj} and persists task_obj.is_final
        - if tasks_or_task_ids is a list: expects [task_id, ...] and persists is_final as False
        """

        with self.Session.begin() as session:

            # NEW PATH: dict {task_id: task_obj}
            if hasattr(tasks_or_task_ids, "items"):
                for i, (task_id, task_obj) in enumerate(tasks_or_task_ids.items()):
                    task_orm = TaskORM(
                        id=task_id,
                        dag_id=dag_id,
                        execution_id=execution_id,
                        status="pending",
                        insertion_order=i,
                        is_final=(
                            "True" if getattr(task_obj, "is_final", False) else "False"
                        ),
                    )
                    session.add(task_orm)
                return

            # OLD PATH: list [task_id, ...]
            for i, task_id in enumerate(tasks_or_task_ids):
                task_orm = TaskORM(
                    id=task_id,
                    dag_id=dag_id,
                    execution_id=execution_id,
                    status="pending",
                    insertion_order=i,
                    is_final="False",
                )
                session.add(task_orm)

    # ----------------------------------------------------------------------
    # Method: update_dag_execution_status
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
                .filter_by(dag_id=dag_id, id=execution_id)
                .first()
            )
            if execution:
                # If the DAG transitions from "created" to "running" and
                # `started_at` is not set yet, set it now.
                if status == "running" and execution.started_at is None:
                    execution.started_at = datetime.now()
                    execution.thread_id = str(threading.current_thread().ident)
                    execution.pid = threading.current_thread().ident

                execution.status = status

                if status in ["completed", "failed", "cancelled"]:
                    execution.completed_at = datetime.now()

    # ----------------------------------------------------------------------
    # Method: mark_incomplete_tasks_as_failed
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
    # Method: get_running_dags
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
    # Method: get_dags_by_status
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
    # Method: get_all_dags
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
                            latest_execution.status if latest_execution else "created"
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
    # Method: get_dag_summary
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
                # Mark the execution as cancelled
                execution.status = "cancelled"
                execution.completed_at = datetime.now()

                # Mark all incomplete tasks (running, pending) as cancelled
                session.query(TaskORM).filter(
                    TaskORM.dag_id == dag_id,
                    TaskORM.execution_id == execution.id,
                    TaskORM.status.in_(["running", "pending"]),
                ).update(
                    {TaskORM.status: "cancelled", TaskORM.completed_at: datetime.now()},
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
                        "task_id": task.id,
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
    # Method: log_message
    def log_message(
        self,
        dag_id: str,
        execution_id: str,
        task_id: str,
        level: str,
        message: str,
        timestamp: Optional[datetime] = None,
    ):
        """Store a log message with an optional externally provided timestamp."""
        with self.Session.begin() as session:
            log = LogORM(
                dag_id=dag_id,
                execution_id=execution_id,
                task_id=task_id,
                level=level,
                message=message,
                timestamp=timestamp or datetime.now(),
                thread_id=str(threading.current_thread().ident),
            )
            session.add(log)

    # ----------------------------------------------------------------------
    # Method: add_log
    def add_log(
        self,
        dag_id: str,
        execution_id: str,
        task_id: str,
        message: str,
        level: str = "INFO",
        timestamp: Optional[datetime] = None,
    ):
        """Helper wrapper for log_message() with timestamp support."""
        self.log_message(
            dag_id=dag_id,
            execution_id=execution_id,
            task_id=task_id,
            level=level,
            message=message,
            timestamp=timestamp,
        )

    # ----------------------------------------------------------------------
    # Method: get_execution_logs
    def get_execution_logs(
        self, dag_id: str, execution_id: str = None, limit: int = 100
    ) -> List[Dict[str, Any]]:
        """Return the most recent logs for a DAG/execution up to `limit`.

        Logs are returned ordered by timestamp descending.
        """
        with self.Session() as session:
            query = session.query(LogORM).filter_by(dag_id=dag_id)
            if execution_id:
                query = query.filter_by(execution_id=execution_id)
            logs = query.order_by(LogORM.timestamp.desc()).limit(limit).all()
            return [
                {
                    "task_id": log.task_id,
                    "level": log.level,
                    "message": log.message,
                    "timestamp": log.timestamp.isoformat(),
                    "thread_id": log.thread_id,
                }
                for log in logs
            ]

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
    # Method: get_latest_execution
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
    # Method: save_dag_definition
    def save_dag_definition(self, dag, dag_filepath: Optional[str] = None):
        """Persist the DAG definition JSON into the `DagORM.definition` field.

        If the DagORM does not exist a new record is created. `dag` is
        expected to expose a `dag_id` attribute and `to_dict()` method.
        """
        with self.Session.begin() as session:
            dag_orm = session.query(DagORM).filter_by(id=dag.dag_id).first()
            if not dag_orm:
                dag_orm = DagORM(id=dag.dag_id)
                session.add(dag_orm)
            dag_orm.definition = json.dumps(dag.to_dict())
            if dag_filepath:
                dag_orm.dag_filepath = dag_filepath

    # ----------------------------------------------------------------------
    # Method: get_dag_definition
    def get_dag_definition(self, dag_id: str) -> Optional[Dict]:
        """Return the stored DAG definition as a Python object.

        If the stored value is valid JSON it is decoded; otherwise the raw
        stored value is returned (for backward compatibility).
        """
        with self.Session() as session:
            dag = session.query(DagORM).filter_by(id=dag_id).first()
            if not dag:
                return None

            if dag.definition is None:
                return None

            try:
                return json.loads(dag.definition)
            except (TypeError, json.JSONDecodeError):
                # If the stored definition is not valid JSON, fall back to returning the raw value
                return dag.definition

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

    def get_nearest_upstream_timestamp(
        self,
        dag_id: str,
        execution_id: str,
        task_id: str,
        dag,
    ) -> Optional[datetime]:
        """
        Return the most recent valid timestamp among the given task or its upstream
        dependencies (recursively).

        Used to determine an effective timestamp for exit-tasks that never started
        (e.g. skipped tasks).
        """

        visited = set()

        def _walk(tid: str) -> Optional[datetime]:
            if tid in visited:
                return None
            visited.add(tid)

            with self.Session() as session:
                task = (
                    session.query(TaskORM)
                    .filter_by(
                        dag_id=dag_id,
                        id=tid,
                        execution_id=execution_id,
                    )
                    .first()
                )

                # 1️⃣ Se il task ha un timestamp valido → usalo
                if task:
                    if task.started_at:
                        return task.started_at
                    if task.completed_at:
                        return task.completed_at

            # 2️⃣ Altrimenti risali ricorsivamente dagli upstream
            dag_task = dag.tasks.get(tid)
            if not dag_task:
                return None

            upstream_times = []
            for dep_id in dag_task.dependencies:
                ts = _walk(dep_id)
                if ts:
                    upstream_times.append(ts)

            return max(upstream_times) if upstream_times else None

        return _walk(task_id)

    def has_any_failed_task(self, dag_id: str, execution_id: str) -> bool:
        tasks = self.get_tasks_for_execution(dag_id, execution_id)
        return any(t["status"] == "failed" for t in tasks)

    def update_execution_status_from_final_task(
        self,
        dag_id: str,
        execution_id: str,
    ) -> Optional[str]:
        """
        Imposta executions.status copiando lo status della task is_final=True
        con started_at più recente per la data execution.
        """

        with self.Session.begin() as session:

            final_task = (
                session.query(TaskORM)
                .filter(
                    TaskORM.dag_id == dag_id,
                    TaskORM.execution_id == execution_id,
                    TaskORM.is_final == "True",  # ⚠️ stringa, come da tuo DB
                )
                .order_by(TaskORM.started_at.desc())
                .first()
            )

            if not final_task:
                # Fuori scope: nessuna final task
                return None

            execution = (
                session.query(ExecutionORM)
                .filter_by(dag_id=dag_id, id=execution_id)
                .first()
            )

            if not execution:
                return None

            execution.status = final_task.status
            execution.completed_at = datetime.now()

            return final_task.status
