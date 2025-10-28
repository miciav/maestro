# maestro/src/server/task/bash_task.py

import subprocess
import logging
from maestro.server.tasks.base import BaseTask

logger = logging.getLogger(__name__)

class BashTask(BaseTask):
    """
    A task that executes a Bash command locally.
    """
    command: str

    def execute_local(self):
        logger.info(f"[BashTask] Executing command: {self.command}")
        try:
            result = subprocess.run(
                self.command,
                shell=True,
                capture_output=True,
                text=True
            )
            if result.returncode == 0:
                logger.info(f"[BashTask] Output:\n{result.stdout.strip()}")
                self.status = "completed"
            else:
                logger.error(f"[BashTask] Error:\n{result.stderr.strip()}")
                self.status = "failed"
        except Exception as e:
            logger.exception(f"[BashTask] Exception: {e}")
            self.status = "failed"

    def to_dict(self):
        """Include the command field in serialized form."""
        base = super().to_dict()
        base["command"] = self.command
        return base
