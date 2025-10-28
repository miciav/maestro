from typing import Optional
from pydantic import Field
from maestro.server.tasks.base import BaseTask


class PythonTask(BaseTask):
    """
    Executes a Python snippet provided as a string or a file path.
    """
    code: Optional[str] = Field(default=None, description="Inline Python code to execute")
    script_path: Optional[str] = Field(default=None, description="Path to a .py file to execute")

    def execute_local(self):
        if self.code:
            exec(self.code, {})
        elif self.script_path:
            with open(self.script_path, "r") as f:
                code = f.read()
            exec(code, {})
        else:
            raise ValueError("PythonTask requires either 'code' or 'script_path'.")
