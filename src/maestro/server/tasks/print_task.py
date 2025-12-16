import time
import logging
from typing import Optional
from rich import get_console

from maestro.server.tasks.base import BaseTask
from maestro.server.internals.status_manager import StatusManager


class PrintTask(BaseTask):
    """A task that prints a message to the console."""
    message: str
    delay: Optional[int] = 0

    def execute_local(self):
        """
        PrintTask: registra nel DB lo stesso messaggio che stampa su stdout,
        con timestamp corretto e senza dipendere dal logger di orchestrator.
        """
        sm = StatusManager.get_instance()

        message = f"[PrintTask] {self.message}"

        # stampa su console server (come ora)
        print(message, flush=True)

        # aggiungi al DB
        sm.add_log(
            dag_id=self.dag_id,
            execution_id=self.execution_id,
            task_id=self.task_id,
            message=message,
            level="INFO"
        )
