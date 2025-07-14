
import time

from maestro.tasks.base import BaseTask
from rich import get_console


class WaitTask(BaseTask):
    """A task that waits for a specified amount of time before completing.
    This can be useful for simulating delays or waiting for external conditions.
    """
    delay: int

    def execute(self):
        get_console().print(f"[WaitTask] Waiting for {self.delay} seconds...")
        time.sleep(self.delay)
