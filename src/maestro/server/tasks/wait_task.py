
import time

from maestro.server.tasks.base import BaseTask
from rich import get_console


class WaitTask(BaseTask):
    """A task that waits for a specified amount of time before completing.
    This can be useful for simulating delays or waiting for external conditions.
    """
    wait_seconds: int

    def execute_local(self):
        get_console().print(f"[WaitTask] Waiting for {self.wait_seconds} seconds...")
        time.sleep(self.wait_seconds)
