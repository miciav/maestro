from abc import ABC, abstractmethod
from enum import Enum
from typing import List, Optional, Callable
from pathlib import Path
import logging

from pydantic import BaseModel, Field
from rich.console import Console


class TaskStatus(str, Enum):
    """Enumeration for the status of a task."""
    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    SKIPPED = "skipped"


class Task(BaseModel, ABC):
    """
    The base class for all tasks. It's a Pydantic model to ensure
    type validation and provides the basic attributes every task has.
    """
    task_id: str # Unique identifier for the task
    dag_file_path: Optional[str] = None # Path to the original DAG file
    dependencies: List[str] = Field(default_factory=list)
    status: TaskStatus = TaskStatus.PENDING
    executor: str = "local" # New field for executor type
    on_success: Optional[Callable] = None # Callback for when the task completes successfully
    on_failure: Optional[Callable] = None # Callback for when the task fails

    class Config:
        arbitrary_types_allowed = True

    @abstractmethod
    def execute_local(self):
        """The main execution logic for the task when run locally."""
        pass

    def execute(self, executor_instance):
        """Delegates execution to the specified executor."""
        executor_instance.execute(self)

    def __repr__(self):
        return f"Task(task_id='{self.task_id}', status='{self.status.value}')"

    def to_dict(self):
        """Returns a dictionary representation of the task."""
        task_dict = self.model_dump(exclude={'on_success', 'on_failure'})
        # Add the type field based on the class name
        task_dict['type'] = self.__class__.__name__
        return task_dict
