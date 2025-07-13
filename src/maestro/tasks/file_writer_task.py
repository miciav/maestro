
import os
from typing import Literal

from maestro.tasks.base import BaseTask

class FileWriterTask(BaseTask):
    filepath: str
    content: str
    mode: Literal["append", "overwrite"] = "overwrite"

    def execute(self):
        directory = os.path.dirname(self.filepath)
        if directory:
            os.makedirs(directory, exist_ok=True)
        write_mode = 'a' if self.mode == "append" else 'w'
        with open(self.filepath, write_mode) as f:
            f.write(self.content)
        print(f"[FileWriterTask] Wrote to {self.filepath}")
