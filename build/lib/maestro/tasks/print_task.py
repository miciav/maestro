
import time
from typing import Optional

from maestro.tasks.base import BaseTask

class PrintTask(BaseTask):
    message: str
    delay: Optional[int] = 0

    def execute(self):
        print(f"[PrintTask] {self.message}")
        if self.delay:
            time.sleep(self.delay)
