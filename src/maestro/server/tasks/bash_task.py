import subprocess
import logging
from maestro.server.tasks.base import BaseTask
from maestro.server.internals.status_manager import StatusManager


logger = logging.getLogger(__name__)
logger.propagate = True

class BashTask(BaseTask):
    """
    A task that executes a Bash command locally.
    Captures stdout and stderr and sends them to the logging system.
    """

    command: str


    def execute_local(self):
        """
        BashTask: esegue il comando e registra stdout/stderr nel DB riga per riga,
        con timestamp corretto come PythonTask.
        """
        sm = StatusManager.get_instance()
        dag_id = self.dag_id
        execution_id = self.execution_id
        task_id = self.task_id

        process = subprocess.Popen(
            self.command,
            shell=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            bufsize=1
        )

        # Leggi STDOUT riga per riga
        for line in process.stdout:
            clean = line.rstrip("\n")
            msg = f"[BashTask][stdout] {clean}"

            print(msg, flush=True)

            sm.add_log(
                dag_id=dag_id,
                execution_id=execution_id,
                task_id=task_id,
                message=msg,
                level="INFO"
            )

        # Leggi STDERR riga per riga
        for line in process.stderr:
            clean = line.rstrip("\n")
            msg = f"[BashTask][stderr] {clean}"

            print(msg, flush=True)

            sm.add_log(
                dag_id=dag_id,
                execution_id=execution_id,
                task_id=task_id,
                message=msg,
                level="ERROR"
            )

        # Ritorno exit code
        process.wait()

        if process.returncode != 0:
            sm.add_log(
                dag_id=dag_id,
                execution_id=execution_id,
                task_id=task_id,
                message=f"[BashTask] Failed with exit code {process.returncode}",
                level="ERROR"
            )


    def to_dict(self):
        """Include the command field in serialized form."""
        base = super().to_dict()
        base["command"] = self.command
        return base
