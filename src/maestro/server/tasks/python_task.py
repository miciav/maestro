from typing import Optional
from pydantic import Field
import io
import contextlib
import threading

from maestro.server.tasks.base import BaseTask
from maestro.server.internals.status_manager import StatusManager


class PythonTask(BaseTask):
    """
    Executes a Python snippet or script, and logs any printed output.
    """

    code: Optional[str] = Field(default=None, description="Inline Python code to execute")
    script_path: Optional[str] = Field(default=None, description="Path to a .py file to execute")

    def execute_local(self):
        buffer = io.StringIO()

        with contextlib.redirect_stdout(buffer), contextlib.redirect_stderr(buffer):
            if self.code:
                exec(self.code, {})
            elif self.script_path:
                with open(self.script_path, "r") as f:
                    code = f.read()
                exec(code, {})
            else:
                raise ValueError("PythonTask requires either 'code' or 'script_path'.")

        output = buffer.getvalue().strip()
        if output:
            sm = StatusManager.get_instance()
            sm.add_log(
                dag_id=getattr(self, "dag_id", None),
                execution_id=getattr(self, "execution_id", None),
                task_id=self.task_id,
                message=output,
                level="INFO"
            )
