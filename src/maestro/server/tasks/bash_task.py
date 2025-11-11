import subprocess
import logging
from maestro.server.tasks.base import BaseTask

logger = logging.getLogger(__name__)
logger.propagate = True

class BashTask(BaseTask):
    """
    A task that executes a Bash command locally.
    Captures stdout and stderr and sends them to the logging system.
    """

    command: str

    def execute_local(self):
        logger.info(f"[BashTask] Executing command: {self.command}")
        try:
            result = subprocess.run(
                self.command,
                shell=True,
                executable="/bin/bash",
                capture_output=True,
                text=True
            )

            # --- LOG STDOUT LINE BY LINE ---
            if result.stdout:
                for line in result.stdout.strip().splitlines():
                    logger.info(f"[BashTask][stdout] {line}")

            # --- LOG STDERR LINE BY LINE ---
            if result.stderr:
                for line in result.stderr.strip().splitlines():
                    logger.error(f"[BashTask][stderr] {line}")

            # --- Set task status ---
            if result.returncode == 0:
                logger.info("[BashTask] Command completed successfully.")
                self.status = "completed"
            else:
                logger.error(f"[BashTask] Command failed with return code {result.returncode}.")
                self.status = "failed"

        except Exception as e:
            logger.exception(f"[BashTask] Exception while executing command: {e}")
            self.status = "failed"

    def to_dict(self):
        """Include the command field in serialized form."""
        base = super().to_dict()
        base["command"] = self.command
        return base
