from typing import Optional
from maestro.shared.task import Task

class BaseTask(Task):
    """Base class for a task, inheriting from the core Task which is a Pydantic model."""

    # 🆕 aggiungiamo questi due campi
    dag_id: Optional[str] = None
    execution_id: Optional[str] = None

    def execute_local(self):
        raise NotImplementedError("Subclasses must implement this method.")
