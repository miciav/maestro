import subprocess
import logging
from maestro.server.tasks.base import BaseTask
from maestro.server.internals.status_manager import StatusManager

logger = logging.getLogger(__name__)
logger.propagate = True

class BashTask(BaseTask):
    command: str

    def execute_local(self):
        sm = StatusManager.get_instance()
        dag_id = self.dag_id
        execution_id = self.execution_id
        task_id = self.task_id

        process = subprocess.Popen(
            self.command,
            shell=True,
            executable="/bin/bash",
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            bufsize=1
        )

        captured_output = None

        # STDOUT
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

            # NEW: detect structured output: OUTPUT: ...

            raw = clean.lstrip()
            if raw.startswith("OUTPUT:"):
                value = raw.split("OUTPUT:", 1)[1].strip()
                captured_output = value

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

        # Attendi la fine del processo
        process.wait()
        rc = process.returncode

        if rc != 0:
            msg = f"[BashTask] Failed with exit code {rc}"
            print(msg, flush=True)
            sm.add_log(
                dag_id=dag_id,
                execution_id=execution_id,
                task_id=task_id,
                message=msg,
                level="ERROR"
            )
            raise Exception(f"BashTask failed with exit code {rc}")

        # NEW — persist captured output (if present)
        if captured_output is not None:
            sm.set_task_output(dag_id, task_id, execution_id, captured_output)

        # Success log finale
        success_msg = "[BashTask] Completed successfully (exit code 0)"
        print(success_msg, flush=True)
        sm.add_log(
            dag_id=dag_id,
            execution_id=execution_id,
            task_id=task_id,
            message=success_msg,
            level="INFO"
        )

    def to_dict(self):
        """Include the command field in serialized form."""
        base = super().to_dict()
        base["command"] = self.command
        return base
