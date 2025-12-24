import datetime
import threading

from sqlalchemy import (
    Boolean,
    Column,
    DateTime,
    ForeignKey,
    Integer,
    String,
    Text,
    create_engine,
)
from sqlalchemy.orm import declarative_base, relationship, sessionmaker
from sqlalchemy.sql import func

Base = declarative_base()


# --------------------
# DAG
# --------------------
class DagORM(Base):
    __tablename__ = "dags"

    id = Column(String, primary_key=True)  # nome DAG dal YAML
    definition_hash = Column(Text)
    dag_filepath = Column(String)
    created_at = Column(DateTime, server_default=func.now())
    updated_at = Column(DateTime)
    is_active = Column(Boolean, default=True)

    executions = relationship(
        "ExecutionORM",
        back_populates="dag",
        cascade="all, delete-orphan",
    )


# --------------------
# EXECUTION
# --------------------
class ExecutionORM(Base):
    __tablename__ = "executions"

    id = Column(String, primary_key=True)
    dag_id = Column(String, ForeignKey("dags.id"), nullable=False)

    run_name = Column(String)  # docker-style
    status = Column(String)

    # Stored as ON/OFF to align with DB constraints in the new schema.
    fail_fast = Column(String, default="OFF")

    failed_tasks = Column(Integer, default=0)
    skipped_tasks = Column(Integer, default=0)

    trigger_type = Column(String)  # manual | api | schedule
    triggered_by = Column(String)

    started_at = Column(DateTime)
    completed_at = Column(DateTime)

    thread_id = Column(String)
    pid = Column(Integer)

    dag = relationship("DagORM", back_populates="executions")
    tasks = relationship(
        "TaskORM",
        back_populates="execution",
        cascade="all, delete-orphan",
    )


# --------------------
# TASK
# --------------------
class TaskORM(Base):
    __tablename__ = "tasks"

    id = Column(String, primary_key=True)  # UUID
    task_id = Column(String, nullable=False)  # nome YAML

    dag_id = Column(String, ForeignKey("dags.id"), nullable=False)
    execution_id = Column(String, ForeignKey("executions.id"), nullable=False)

    status = Column(String)
    started_at = Column(DateTime)
    completed_at = Column(DateTime)

    retry_count = Column(Integer, default=0)
    max_retries = Column(Integer, default=0)

    output = Column(Text)
    output_type = Column(String)

    insertion_order = Column(Integer)
    is_final = Column(Boolean, default=False)

    thread_id = Column(String)

    execution = relationship("ExecutionORM", back_populates="tasks")


# --------------------
# TASK ATTEMPT
# --------------------
class TaskAttemptORM(Base):
    __tablename__ = "task_attempts"

    id = Column(String, primary_key=True)

    task_pk = Column(String, ForeignKey("tasks.id"), nullable=False)
    execution_id = Column(String, ForeignKey("executions.id"), nullable=False)

    attempt_number = Column(Integer, nullable=False)

    status = Column(String)
    started_at = Column(DateTime)
    completed_at = Column(DateTime)

    error = Column(Text)

    thread_id = Column(String)
    pid = Column(Integer)


# --------------------
# TASK DEPENDENCY
# --------------------
class TaskDependencyORM(Base):
    __tablename__ = "task_dependencies"

    id = Column(String, primary_key=True)

    dag_id = Column(String, ForeignKey("dags.id"), nullable=False)

    task_id = Column(String, nullable=False)
    upstream_task_id = Column(String, nullable=False)
    downstream_task_id = Column(String, nullable=False)

    condition = Column(Text)
    dependency_policy = Column(String)  # all | any | none


# --------------------
# LOG
# --------------------
class LogORM(Base):
    __tablename__ = "logs"

    id = Column(Integer, primary_key=True, autoincrement=True)

    dag_id = Column(String, ForeignKey("dags.id"), nullable=False)
    execution_id = Column(String, nullable=False)

    task_pk = Column(String, ForeignKey("tasks.id"), nullable=True)
    attempt_id = Column(String, nullable=True)

    level = Column(String)
    message = Column(Text)

    timestamp = Column(
        DateTime,
        default=datetime.datetime.utcnow,
        nullable=False,
    )

    thread_id = Column(String)
    pid = Column(Integer)


def create_db_engine(db_path="maestro.db"):
    return create_engine(f"sqlite:///{db_path}")


def create_session(engine):
    Session = sessionmaker(bind=engine)
    return Session()


def init_db(engine):
    Base.metadata.create_all(engine)
