import time
from typing import Optional
from rich import get_console

from maestro.tasks.base import BaseTask

class PrintTask(BaseTask):
    """A task that prints a message to the console."""
    message: str
    delay: Optional[int] = 0

    def execute(self):
        get_console().print(f"[PrintTask] {self.message}")
        if self.delay:
            time.sleep(self.delay)