from typing import Optional
from pydantic import Field
from maestro.shared.task import Task

class BaseTask(Task):
    """Base class for a task, inheriting from the core Task which is a Pydantic model."""

    # Contesto runtime
    dag_id: Optional[str] = None
    execution_id: Optional[str] = None

    # 🆕 Campi che devono essere letti dallo YAML
    retries: int = Field(default=0, description="Number of retries on failure")
    retry_delay: int = Field(default=0, description="Delay in seconds between retry attempts")

    def execute_local(self):
        raise NotImplementedError("Subclasses must implement this method.")
