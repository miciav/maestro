import time
import logging
from typing import Optional
from rich import get_console

from maestro.server.tasks.base import BaseTask

class PrintTask(BaseTask):
    """A task that prints a message to the console."""
    message: str
    delay: Optional[int] = 0

    def execute_local(self):
        logger = logging.getLogger(__name__)
        logger.info(f"[PrintTask] {self.message}")
        if self.delay:
            time.sleep(self.delay)
