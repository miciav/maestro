from typing import Optional
from pydantic import Field
import io
import contextlib
import threading
import sys

from maestro.server.tasks.base import BaseTask
from maestro.server.internals.status_manager import StatusManager


class PythonTask(BaseTask):
    """
    Executes inline Python code or a .py script.
    Captures print() output and writes it into logs table
    with proper dag_id, execution_id, and formatted message.
    """

    code: Optional[str] = Field(default=None, description="Inline Python code to execute")
    script_path: Optional[str] = Field(default=None, description="Path to a .py file to execute")

    def execute_local(self):
        """
        Esegue il codice Python catturando le chiamate a print() e
        scrivendole DIRETTAMENTE nella tabella logs tramite StatusManager,
        senza toccare sys.stdout/sys.stderr globali (thread-safe).
        """
        sm = StatusManager.get_instance()

        dag_id = getattr(self, "dag_id", None) or "unknown_dag"
        execution_id = getattr(self, "execution_id", None) or "unknown_execution"
        task_id = self.task_id

        # useremo la print "vera" solo per la console server
        real_print = print

        def _log_line(level: str, text: str):
            # stampa su console server
            real_print(text, flush=True)
            # e registra nel DB
            sm.add_log(
                dag_id=dag_id,
                execution_id=execution_id,
                task_id=task_id,
                message=text,
                level=level
            )

        def task_print(*args, **kwargs):
            sep = kwargs.get("sep", " ")
            end = kwargs.get("end", "\n")
            s = sep.join(str(a) for a in args) + end

            # spezza su newline per loggare riga per riga
            for line in s.splitlines():
                clean = line.rstrip("\r")
                if clean.strip():
                    _log_line("INFO", f"[PythonTask] {clean}")

        # Ambiente di esecuzione: print viene shadowata
        env = {
            "__name__": "__main__",
            "print": task_print,
        }

        try:
            if self.code:
                exec(self.code, env)
            elif self.script_path:
                with open(self.script_path, "r") as f:
                    code = f.read()
                exec(code, env)
            else:
                raise ValueError("PythonTask requires either 'code' or 'script_path'.")
        except Exception:
            # logga l'eccezione nel DB
            tb = traceback.format_exc()
            for line in tb.splitlines():
                if line.strip():
                    _log_line("ERROR", f"[PythonTask][exception] {line}")
            # rialza per far fallire il task
            raise
