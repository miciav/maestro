
import time

from maestro.tasks.base import BaseTask
from rich import get_console


class WaitTask(BaseTask):
    delay: int

    def execute(self):
        get_console().print(f"[WaitTask] Waiting for {self.delay} seconds...")
        time.sleep(self.delay)
