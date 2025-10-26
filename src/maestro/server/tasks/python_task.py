from maestro.server.tasks.base import BaseTask
from maestro.server.internals.status_manager import StatusManager

class PythonTask(BaseTask):
    """Task that executes a Python function or code snippet."""

    def __init__(self, task_id: str, code: str = None, function: str = None, **kwargs):
        super().__init__(task_id=task_id, **kwargs)
        self.code = code
        self.function = function

    def run(self, **context):
        """Execute the Python code or function dynamically."""
        self.log(f"Executing PythonTask: {self.task_id}")

        if self.code:
            # Execute provided code string safely
            local_ctx = {}
            exec(self.code, {}, local_ctx)
            result = local_ctx.get("result")
            self.log(f"PythonTask result: {result}")
            return result

        elif self.function:
            # Import and run a Python callable by dotted path
            module_name, func_name = self.function.rsplit(".", 1)
            module = __import__(module_name, fromlist=[func_name])
            func = getattr(module, func_name)
            result = func(**context)
            self.log(f"PythonTask function result: {result}")
            return result

        else:
            self.log("No code or function provided.")
            return None
