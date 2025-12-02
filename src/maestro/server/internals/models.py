
from sqlalchemy import create_engine, Column, String, ForeignKey, DateTime, Integer, Text
from sqlalchemy.orm import sessionmaker, relationship, declarative_base
from sqlalchemy.sql import func
import threading
import datetime

Base = declarative_base()

class DagORM(Base):
    __tablename__ = 'dags'

    id = Column(String, primary_key=True)
    definition = Column(Text)
    dag_filepath = Column(String)
    created_at = Column(DateTime, server_default=func.now())
    executions = relationship("ExecutionORM", back_populates="dag", cascade="all, delete-orphan")

class ExecutionORM(Base):
    __tablename__ = 'executions'

    id = Column(String, primary_key=True)
    dag_id = Column(String, ForeignKey('dags.id'), primary_key=True)
    status = Column(String)
    started_at = Column(DateTime)
    completed_at = Column(DateTime)
    thread_id = Column(String)
    pid = Column(Integer)
    dag = relationship("DagORM", back_populates="executions")
    tasks = relationship("TaskORM", back_populates="execution", cascade="all, delete-orphan")

class TaskORM(Base):
    __tablename__ = 'tasks'

    id = Column(String, primary_key=True)
    dag_id = Column(String, ForeignKey('dags.id'), primary_key=True)
    execution_id = Column(String, ForeignKey('executions.id'), primary_key=True)
    status = Column(String)
    started_at = Column(DateTime)
    completed_at = Column(DateTime)
    thread_id = Column(String)
    output = Column(Text)
    insertion_order = Column(Integer)
    execution = relationship("ExecutionORM", back_populates="tasks")

class LogORM(Base):
    __tablename__ = 'logs'

    id = Column(Integer, primary_key=True, autoincrement=True)
    dag_id = Column(String, ForeignKey('dags.id'))
    execution_id = Column(String)
    task_id = Column(String)
    level = Column(String)
    message = Column(Text)
    timestamp = Column(
        DateTime,
        default=datetime.datetime.utcnow,      # ✔ corretto
        nullable=False
    )
    thread_id = Column(String)

def create_db_engine(db_path='maestro.db'):
    return create_engine(f'sqlite:///{db_path}')

def create_session(engine):
    Session = sessionmaker(bind=engine)
    return Session()

def init_db(engine):
    Base.metadata.create_all(engine)
