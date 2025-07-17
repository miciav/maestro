from maestro.core.task import Task

class BaseTask(Task):
    """Base class for a task, inheriting from the core Task which is a Pydantic model."""
    def execute_local(self):
        raise NotImplementedError("Subclasses must implement this method.")