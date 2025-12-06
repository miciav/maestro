import threading
from datetime import datetime, timedelta

import pytest
from sqlalchemy import inspect

from maestro.server.internals.models import Base, DagORM, ExecutionORM, LogORM, TaskORM
from maestro.server.internals.status_manager import StatusManager


# -----------------------------
# Fixture: StatusManager pulito
# -----------------------------
@pytest.fixture
def status_manager():
    # Reset singleton
    StatusManager._instance = None
    sm = StatusManager(":memory:")  # DB in-memory
    yield sm
    StatusManager._instance = None


# -----------------------------
# Test: Singleton e inizializzazione DB
# -----------------------------
def test_singleton_instance(status_manager):
    # Dopo init, get_instance deve restituire lo stesso oggetto
    instance = StatusManager.get_instance()
    assert instance is status_manager


def test_db_tables_created(status_manager):
    inspector = inspect(status_manager.engine)
    tables = inspector.get_table_names()

    from maestro.server.internals.models import DagORM, ExecutionORM, LogORM, TaskORM

    expected_tables = [
        DagORM.__tablename__,
        ExecutionORM.__tablename__,
        TaskORM.__tablename__,
        LogORM.__tablename__,
    ]

    for tbl in expected_tables:
        assert tbl in tables


# -----------------------------
# Test: DAG execution lifecycle
# -----------------------------
def test_create_and_update_execution(status_manager):
    dag_id = "dag1"
    exec_id = "exec1"

    # Create execution
    status_manager.create_dag_execution(dag_id, exec_id)
    latest = status_manager.get_latest_execution(dag_id)
    assert latest["execution_id"] == exec_id
    assert latest["status"] == "running"

    # Update status to completed
    status_manager.update_dag_execution_status(dag_id, exec_id, "completed")
    latest = status_manager.get_latest_execution(dag_id)
    assert latest["status"] == "completed"
    assert latest["completed_at"] is not None


# -----------------------------
# Test: Task status and output
# -----------------------------
def test_set_and_get_task_status_and_output(status_manager):
    dag_id = "dag1"
    exec_id = "exec1"
    task_id = "task1"

    # Initialize task
    status_manager.initialize_tasks_for_execution(dag_id, exec_id, [task_id])

    # Set task status
    status_manager.set_task_status(dag_id, task_id, "running", exec_id)
    status = status_manager.get_task_status(dag_id, task_id, exec_id)
    assert status == "running"

    # Set output
    output_data = {"result": 42}
    status_manager.set_task_output(dag_id, task_id, exec_id, output_data)
    output = status_manager.get_task_output(dag_id, task_id, exec_id)
    assert output == output_data


# -----------------------------
# Test: DAG status retrieval
# -----------------------------
def test_get_dag_status_and_reset(status_manager):
    dag_id = "dag1"
    exec_id = "exec1"
    tasks = ["t1", "t2"]

    status_manager.initialize_tasks_for_execution(dag_id, exec_id, tasks)
    status_manager.set_task_status(dag_id, "t1", "completed", exec_id)
    status_manager.set_task_status(dag_id, "t2", "running", exec_id)

    dag_status = status_manager.get_dag_status(dag_id, exec_id)
    assert dag_status == {"t1": "completed", "t2": "running"}

    # Reset DAG status
    status_manager.reset_dag_status(dag_id, exec_id)
    dag_status = status_manager.get_dag_status(dag_id, exec_id)
    assert dag_status == {}


# -----------------------------
# Test: Logging
# -----------------------------
def test_logs(status_manager):
    dag_id = "dag1"
    exec_id = "exec1"
    task_id = "task1"

    # Log a message
    status_manager.log_message(dag_id, exec_id, task_id, "INFO", "Test log")
    logs = status_manager.get_execution_logs(dag_id, exec_id)
    assert len(logs) == 1
    assert logs[0]["message"] == "Test log"


# -----------------------------
# Test: DAG definitions
# -----------------------------
class DummyDag:
    def __init__(self, dag_id):
        self.dag_id = dag_id

    def to_dict(self):
        return {"dag_id": self.dag_id, "tasks": []}


def test_save_and_get_dag_definition(status_manager):
    dag_id = "dag1"
    dag = DummyDag(dag_id)
    status_manager.save_dag_definition(dag)
    definition = status_manager.get_dag_definition(dag_id)
    assert definition["dag_id"] == dag_id


# -----------------------------
# Test: DAG deletion
# -----------------------------
def test_delete_dag(status_manager):
    dag_id = "dag_to_delete"
    dag = DummyDag(dag_id)
    status_manager.save_dag_definition(dag)

    # Delete DAG
    deleted = status_manager.delete_dag(dag_id)
    assert deleted == 1
    # Check non-existent DAG
    deleted = status_manager.delete_dag("nonexistent")
    assert deleted == 0


# -----------------------------
# Test: DAG id validation and uniqueness
# -----------------------------
def test_dag_id_validation_and_uniqueness(status_manager):
    valid_id = "dag_123"
    invalid_id = "dag 123!"
    assert status_manager.validate_dag_id(valid_id) is True
    assert status_manager.validate_dag_id(invalid_id) is False

    # Uniqueness
    dag = DummyDag("unique_dag")
    status_manager.save_dag_definition(dag)
    assert status_manager.check_dag_id_uniqueness("unique_dag") is False
    assert status_manager.check_dag_id_uniqueness("new_dag") is True


# -----------------------------
# Test: Cleanup old executions
# -----------------------------
def test_cleanup_old_executions(status_manager):
    dag_id = "dag1"
    exec_id = "old_exec"
    old_time = datetime.now() - timedelta(days=60)

    # Create old execution manually
    with status_manager.Session.begin() as session:
        session.add(DagORM(id=dag_id))
        session.add(
            ExecutionORM(
                id=exec_id, dag_id=dag_id, status="completed", started_at=old_time
            )
        )

    count = status_manager.cleanup_old_executions(days_to_keep=30)
    assert count == 1


# -----------------------------
# Test: Cancel DAG execution
# -----------------------------
def test_cancel_dag_execution(status_manager):
    dag_id = "dag1"
    exec_id = "exec1"
    task_ids = ["t1", "t2"]

    status_manager.create_dag_execution(dag_id, exec_id)
    status_manager.initialize_tasks_for_execution(dag_id, exec_id, task_ids)
    status_manager.set_task_status(dag_id, "t1", "running", exec_id)

    cancelled = status_manager.cancel_dag_execution(dag_id, exec_id)
    assert cancelled is True
    dag_status = status_manager.get_dag_status(dag_id, exec_id)
    assert all(status == "cancelled" for status in dag_status.values())


# -----------------------------
# Test: Generate unique DAG id
# -----------------------------
def test_generate_unique_dag_id(status_manager):
    dag_id = status_manager.generate_unique_dag_id()
    assert isinstance(dag_id, str)
    assert status_manager.validate_dag_id(dag_id)


# -----------------------------
# Test: Get DAG summary
# -----------------------------
def test_get_dag_summary(status_manager):
    dag_id = "dag1"
    exec_id = "exec1"
    status_manager.create_dag_execution(dag_id, exec_id)
    summary = status_manager.get_dag_summary()
    assert "total_executions" in summary
    assert "unique_dags" in summary
    assert "status_counts" in summary


# -----------------------------
# Test extra: fallback get_task_status
# -----------------------------
def test_task_status_fallback(status_manager):
    dag_id = "dag_fallback"
    exec_id = "exec1"
    task_id = "task1"

    # Inseriamo un task globale (execution_id=None)
    status_manager.initialize_tasks_for_execution(dag_id, None, [task_id])
    status_manager.set_task_status(dag_id, task_id, "completed", execution_id=None)

    # Chiediamo lo status per un execution-specific task inesistente
    status = status_manager.get_task_status(dag_id, task_id, execution_id=exec_id)
    assert status == "completed"  # deve ripiegare sul task globale


# -----------------------------
# Test extra: multi-thread execution
# -----------------------------
import threading


def test_multithreaded_execution(status_manager):
    dag_id = "dag_thread"
    exec_ids = ["exec1", "exec2"]

    def create_exec(exec_id):
        status_manager.create_dag_execution(dag_id, exec_id)

    threads = [threading.Thread(target=create_exec, args=(eid,)) for eid in exec_ids]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    # Controlla che entrambe le execution siano presenti
    executions = status_manager.get_all_dags()
    latest_exec_ids = [d["execution_id"] for d in executions if d["execution_id"]]
    for eid in exec_ids:
        assert eid in latest_exec_ids

# -----------------------------
# Test extra: insertion_order dei task
# -----------------------------
def test_task_insertion_order(status_manager):
    dag_id = "dag_order"
    exec_id = "exec_order"
    tasks = ["t3", "t1", "t2"]  # ordine arbitrario

    status_manager.initialize_tasks_for_execution(dag_id, exec_id, tasks)
    details = status_manager.get_dag_execution_details(dag_id, exec_id)
    retrieved_order = [t["task_id"] for t in details["tasks"]]
    # L'ordine deve rispettare insertion_order
    assert retrieved_order == tasks
