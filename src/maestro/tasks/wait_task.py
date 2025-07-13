
import time

from maestro.tasks.base import BaseTask

class WaitTask(BaseTask):
    delay: int

    def execute(self):
        print(f"[WaitTask] Waiting for {self.delay} seconds...")
        time.sleep(self.delay)
