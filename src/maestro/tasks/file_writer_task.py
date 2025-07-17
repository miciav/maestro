import os
from typing import Literal
from rich import get_console

from maestro.tasks.base import BaseTask

class FileWriterTask(BaseTask):
    """A task that writes content to a file."""
    filepath: str
    content: str
    mode: Literal["append", "overwrite"] = "overwrite" # Mode to write to the file, either append or overwrite

    def execute_local(self):
        directory: str = os.path.dirname(self.filepath)
        if directory:
            os.makedirs(directory, exist_ok=True)
        write_mode = 'a' if self.mode == "append" else 'w'
        with open(self.filepath, write_mode) as f:
            f.write(self.content)
        get_console().print(f"[FileWriterTask] Wrote to {self.filepath}")